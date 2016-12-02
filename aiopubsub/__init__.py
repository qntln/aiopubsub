from typing import Tuple, Any, Callable, Set, Union, NamedTuple, Iterable

import asyncio
import collections

import aiopubsub.loop
from aiopubsub.logging_compat import get_logger



PossibleKey = Union[str, Iterable[str]]



class Key(tuple):

	def __new__ (cls, *components: str) -> 'Key':
		k = super().__new__(cls, tuple(components))
		k.is_wildcard = any(part == '*' for part in k)
		return k


	def is_subset_of(self, another: 'Key') -> bool:
		'''
		Is this key a special case of ``another`` key?
			1. If ``another`` is a wildcard, return true when this key shares the same prefix up to the wildcard terminus.
			E.g. ('a', 'b') is_subset_of ('a', '*') == True. ('b', 'b') is_subset_of ('a', '*') == False.
			2. If ``another`` is not a wildcard return true iff the two keys are equal.
		'''
		if not another.is_wildcard:
			return self == another
		if len(self) < len(another):
			return False
		if len(self) > len(another) and another[-1] != '*':
			return False

		for s, a in zip(self, another):
			if a != '*' and s != a:
				return False

		return True


	def __add__(self, other: Iterable[str]) -> 'Key':
		return Key(*(tuple(self) + tuple(other)))


	@classmethod
	def create_from(cls, x: PossibleKey) -> 'Key':
		'''
		Create a Key instance from either an iterable of strings (then result = Key(*x)) or a string (result = Key(x)).
		'''
		if isinstance(x, str):
			return cls(x)
		return cls(*x)



class Hub:

	def __init__(self) -> None:
		self._logger = get_logger(self.__class__.__name__)
		self._subscribers = collections.defaultdict(list)


	def publish(self, key: Key, message: Any) -> None:
		'''
		:raises: asyncio.QueueFull if any of the subscribers' queues overflows - FIXME - should we handle this more gracefully?
		'''
		for sub_key, qs in self._subscribers.items():
			if key.is_subset_of(sub_key):
				for q in qs:
					q.put_nowait((key, message))


	def add_subscriber(self, key: Key, queue: asyncio.Queue) -> None:
		'''
		Register a subscription for a particular key.

		:param key: Filter by this message identifier.
		:param queue: A queue object that will receive all messages for this subscription.
		'''
		self._logger.debug('Subscription to key = %r.', key)
		if queue in self._subscribers[key]:
			self._logger.warning('Repeated subscription to key = %r.', key)
		else:
			self._subscribers[key].append(queue)
			k = Key('Hub', 'addedSubscriber') + key
			self.publish(k, {'key': key, 'currentSubscriberCount': len(self._subscribers[key])})


	def remove_subscriber(self, key: Key, queue: asyncio.Queue) -> None:
		'''
		Unregister a subscription previously created by `add_subscriber`.
		'''
		if queue in self._subscribers[key]:
			self._logger.debug('Canceled subscription to key = %r.', key)
			self._subscribers[key].remove(queue)
			k = Key('Hub', 'removedSubscriber') + key
			self.publish(k, {'key': key, 'currentSubscriberCount': len(self._subscribers[key])})
		else:
			self._logger.warning('Unsubscribe from nonexistent subscription to key = %r.', key)


	def has_subscribers(self, key: Key = Key('*')) -> bool:
		'''
		When `key` is Key('*'), indicate if there are any subscribers at all.
		When `key` is provided, indicate if there are any subscribers for that particular key.
		'''
		return any((sub_key.is_subset_of(key) and qs) for sub_key, qs in self._subscribers.items())



class Publisher:

	def __init__(self, hub: Hub, prefix: PossibleKey) -> None:
		self._hub = hub
		self.prefix = Key.create_from(prefix)


	def publish(self, key: PossibleKey, message: Any) -> None:
		'''
		Push the `message` to subscribers that are subscribed to this `key`. Publisher ID is prepended to the key.
		'''
		full_key = self.prefix + Key.create_from(key)
		self._hub.publish(full_key, message)


	def has_subscribers(self, key: PossibleKey = Key('*')) -> bool:
		'''
		When `key` is Key('*'), indicate if there are any subscribers at all to this publisher.
		When a certain `key` is provided, indicate if there are any subscribers for that particular key of this publisher.
		'''
		full_key = self.prefix + Key.create_from(key)
		return self._hub.has_subscribers(full_key)



Listener = NamedTuple('Listener', [('loop', aiopubsub.loop.Loop), ('keys', Set[Key])])
ListenerCallback = Callable[[Key, Any], None]


class Subscriber:

	'''
	Subscriptions are created using this message queue.
	'''
	QUEUE_CLASS = asyncio.Queue


	'''
	By default, subscriptions will be created and consumed with this ID.
	'''
	DEFAULT_SUBSCRIPTION_ID = 'default'


	def __init__(self, hub: Hub, id: str) -> None:
		self.id = id
		self._logger = get_logger('{0}({1})'.format(self.__class__.__name__, self.id))
		self._hub = hub
		self._subscriptions = {} # type: Dict[str, asyncio.Queue]
		self._listeners = {} # type: Dict[str, Listener]


	def subscribe(self, key: Key, subscription_id: str = DEFAULT_SUBSCRIPTION_ID) -> None:
		'''
		Subscribe to a particular `key`.
		A single queue is used for all subscriptions with this `subscription_id` (possibly to multiple keys).
		'''
		key = Key.create_from(key)
		self._logger.debug('Creating subscription id = %s to key = %r.', subscription_id, key)
		if subscription_id in self._subscriptions:
			q = self._subscriptions[subscription_id]
		else:
			q = self.QUEUE_CLASS()
			self._subscriptions[subscription_id] = q
		self._hub.add_subscriber(key, q)


	def unsubscribe(self, key: Key, subscription_id: str = DEFAULT_SUBSCRIPTION_ID) -> None:
		'''
		Unsubscribe from data previously subscribed to by `subscribe`.
		'''
		key = Key.create_from(key)
		self._logger.debug('Removing subscription id = %s to key = %r.', subscription_id, key)
		q = self._subscriptions[subscription_id]
		self._hub.remove_subscriber(key, q)


	async def consume(self, subscription_id: str = DEFAULT_SUBSCRIPTION_ID) -> Tuple[Key, Any]:
		'''
		Consume a message from the subscription identified by `subscription_id`.

		:returns: (key, message)
		'''
		key, msg = await self._subscriptions[subscription_id].get()
		return key, msg


	def add_listener(self, keys: Union[Key, Set[Key]], callback: ListenerCallback) -> None:
		'''
		Attach ``callback`` to one or more keys. If more than one key is provided, the callback will be
		reading all messages from one queue, and they are guaranteed to arrive in the same order they were published.

		:param keys: One key, or a set of keys.
		'''
		keys, sub_id = self._get_listener_subscription(keys, callback)
		for key in keys:
			self.subscribe(key, sub_id)

		async def consumer() -> None:
			key, msg = await self.consume(sub_id)
			callback(key, msg)

		loop = aiopubsub.loop.Loop(consumer, delay = None)
		loop.start()
		self._listeners[sub_id] = Listener(loop, keys)


	def remove_listener(self, keys: Union[Key, Set[Key]], callback: ListenerCallback) -> None:
		'''
		Both ``keys`` and ``callback`` must be exactly as passed to :meth:`add_listener` before
		so that the subscription can be found.

		:param keys: One key, or a set of keys.
		'''
		keys, sub_id = self._get_listener_subscription(keys, callback)
		self._stop_listener(sub_id, keys)
		del self._listeners[sub_id]


	def remove_all_listeners(self) -> None:
		'''
		Remove all listeners from the subscriber.
		'''
		for subscription_id, listener in self._listeners.items():
			self._stop_listener(subscription_id, listener.keys)
		self._listeners.clear()


	def _stop_listener(self, subscription_id: str, keys: Set[Key]) -> None:
		'''
		Unsubscribe and stop listener specified by ``subscription_id`` with ``keys``.
		'''
		for key in keys:
			self.unsubscribe(key, subscription_id)
		self._listeners[subscription_id].loop.stop()


	@staticmethod
	def _get_listener_subscription(keys: Union[Key, Set[Key]], callback: ListenerCallback) -> Tuple[Set[Key], str]:
		'''
		Normalize `keys` to a set of :class:`Key`s and create subscription ID from the keys and the callback.

		:returns: (set of Keys, subscription ID)
		'''
		if not isinstance(keys, (set, frozenset)):
			keys = {keys,}
		sub_id = 'callback-{!s}-{!r}'.format(sorted(keys), callback)
		keys = set(map(Key.create_from, keys))
		return keys, sub_id
