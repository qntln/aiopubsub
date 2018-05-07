from typing import Tuple, Any, Callable, Set, Union, NamedTuple, Iterable, Dict, List, DefaultDict, Awaitable, Optional # NOQA

import asyncio
import collections

import aiopubsub.loop
from aiopubsub.logging_compat import get_logger



PossibleKey = Union[str, Iterable[str]]



class Key(tuple):

	def __new__(cls, *components: str) -> 'Key':
		# We use this instead of __init__ with tuple variable in order to get better performance.
		# However, this confuse mypy type checking so we ignore it.
		k = super().__new__(cls, components) # type: ignore
		k.is_wildcard = any(part == '*' for part in k)
		return k


	def is_subset_of(self, another: 'Key') -> bool:
		'''
		Is this key a special case of ``another`` key?
			1. If ``another`` is a wildcard, return true when this key shares the same prefix up to the wildcard terminus.
			E.g. ('a', 'b') is_subset_of ('a', '*') == True. ('b', 'b') is_subset_of ('a', '*') == False.
			2. If ``another`` is not a wildcard return true iff the two keys are equal.
		'''
		if not another.is_wildcard: # type: ignore
			return self == another
		len_self = len(self)
		len_another = len(another)
		if len_self < len_another:
			return False
		if len_self > len_another and another[-1] != '*':
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
			return cls(x) # type: ignore # See Key()
		return cls(*x) # type: ignore # See Key()



Listener = NamedTuple('Listener', [('loop', Optional[aiopubsub.loop.Loop]), ('keys', Set[Key])])
ListenerSyncCallback = Callable[[Key, Any], None]
ListenerAsyncCallback = Callable[[Key, Any], Awaitable[None]]
ListenerCallback = Union[ListenerAsyncCallback, ListenerSyncCallback]
UnreadCallback = Callable[..., int] # callback function from Subscriber for counting unread messages
Subscription = NamedTuple('subscription', [
	('callback', ListenerCallback), ('queue', Optional[asyncio.Queue]), ('unread_callback', Optional[UnreadCallback])
])



class ConsumeQueueNotFoundError(Exception):
	pass



class Hub:

	def __init__(self) -> None:
		self._logger = get_logger(self.__class__.__name__)
		self._subscribers = collections.defaultdict(list) # type: DefaultDict[Key, List[ListenerCallback]]
		# Warning: the cache has no timeout, so if some keys should disappear, we should turn this into LRU cache
		# Note: assuming wildcards are never published, this cache will never contain any wildcard key.
		self._key_callbacks_cache = {} # type: Dict[Key, List[ListenerCallback]]
		# Collection of callback function which counts unread messages
		self._unreads = collections.defaultdict(list) # type: DefaultDict[Key, List[UnreadCallback]]


	def _remove_matching_keys_from_cache(self, key: Key) -> None:
		'''
		Remove cache keys that are subset of given key. Key can be wildcard, cached key can not.
		'''
		# Copy the dict keys so we can change the dict
		for cached_key in list(self._key_callbacks_cache.keys()):
			if cached_key.is_subset_of(key):
				del self._key_callbacks_cache[cached_key]


	def _get_callbacks_by_key(self, key: Key):
		callbacks_list = [] # type: List[ListenerCallback]
		try:
			# Cache hit
			callbacks_list = self._key_callbacks_cache[key]
		except KeyError:
			# Cache miss
			assert not key.is_wildcard # type: ignore # See Key()
			for sub_key, callbacks in self._subscribers.items():
				if key.is_subset_of(sub_key):
					callbacks_list.extend(callbacks)
			self._key_callbacks_cache[key] = callbacks_list
		return callbacks_list


	def publish(self, key: Key, message: Any) -> None:
		for callback in self._get_callbacks_by_key(key):
			callback(key, message)


	def add_subscriber(self, key: Key, callback: ListenerCallback, unread: Optional[UnreadCallback] = None) -> None:
		'''
		Register a subscription for a particular key.

		:param key: Filter by this message identifier.
		:param callback: A callback function that will return all messages for this subscription.
		:param unread: A callback function that will return number of unread messages for this subscription.
		'''
		self._logger.debug('Subscription to key = %r.', key)
		if callback in self._subscribers[key]:
			self._logger.warning('Repeated subscription to key = %r.', key)
		else:
			self._subscribers[key].append(callback)
			if unread is not None:
				self._unreads[key].append(unread)
			key_without_wildcards = (k if k != '*' else r'\*' for k in key)
			k = Key('Hub', 'addedSubscriber') + key_without_wildcards # type: ignore # See Key()
			self.publish(k, {'key': key, 'currentSubscriberCount': len(self._subscribers[key])})

		# Invalidate cache
		self._remove_matching_keys_from_cache(key)


	def remove_subscriber(self, key: Key, callback: ListenerCallback, unread: Optional[UnreadCallback] = None) -> None:
		'''
		Unregister a subscription previously created by `add_subscriber`.
		'''
		if callback in self._subscribers[key]:
			self._logger.debug('Canceled subscription to key = %r.', key)
			self._subscribers[key].remove(callback)
			if unread is not None:
				self._unreads[key].remove(unread)
			key_without_wildcards = (k if k != '*' else r'\*' for k in key)
			k = Key('Hub', 'removedSubscriber') + key_without_wildcards # type: ignore # See Key()
			self.publish(k, {'key': key, 'currentSubscriberCount': len(self._subscribers[key])})
		else:
			self._logger.warning('Unsubscribe from nonexistent subscription to key = %r.', key)

		# Invalidate cache
		self._remove_matching_keys_from_cache(key)


	def has_subscribers(self, key: Key = Key('*')) -> bool:
		'''
		When `key` is Key('*'), indicate if there are any subscribers at all.
		When `key` is provided, indicate if there are any subscribers for that particular key.
		'''
		if self._key_callbacks_cache.get(key):
			return True
		return any((sub_key.is_subset_of(key) and qs) for sub_key, qs in self._subscribers.items())


	def get_subscriber_queue_sizes(self, key: Key) -> Tuple[int, ...]:
		'''
		Asynchronous listeners use queues for storing unread messages.
		When ``subscriber`` is added to ``hub`, it has ``Unread`` callback function,
		stored in ``self._unreads``. It can be used to find number of unread messages
		for subscribers on some ``keys``.

		Returns current queue sizes of all subscribers subscribed to ``key`` and all its
		``subkeys``.
		'''
		assert not key.is_wildcard # type: ignore # See Key()
		sizes_list = [] # type: List[int]
		for sub_key, callbacks in self._unreads.items():
			if key.is_subset_of(sub_key):
				for callback in callbacks:
					sizes_list.append(callback())
		return tuple(sizes_list)


	def get_all_subscriber_queue_sizes(self) -> Dict[Key, Tuple[int, ...]]:
		'''
		Asynchronous listeners use queues for storing unread messages.
		When ``subscriber`` is added to ``hub`, it has ``Unread`` callback function,
		stored in ``self._unreads``. It can by used to find number of unread messages
		for subscribers on some ``keys``.

		Returns a dict of all keys that have subscribers, and the queue sizes of each key's subscribers.
		'''
		return { key: tuple(unread() for unread in unreads) for key, unreads in self._unreads.items() }



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



class Subscriber:

	'''
	Subscriptions are created using this message queue.
	'''


	# By default, subscriptions will be created and consumed with this ID.
	DEFAULT_SUBSCRIPTION_ID = 'default'


	def __init__(self, hub: Hub, id_: str) -> None:
		self.id = id_
		self._logger = get_logger('{0}({1})'.format(self.__class__.__name__, self.id))
		self._hub = hub
		self._subscriptions = {} # type: Dict[str, Subscription]
		self._listeners = {} # type: Dict[str, Listener]
		self._listeners_lock = asyncio.Lock()


	def sync_subscribe(self, key: Key, callback, subscription_id: str = DEFAULT_SUBSCRIPTION_ID) -> None:
		'''
		Subscribe to a particular `key`.
		'''
		key = Key.create_from(key)
		self._logger.debug('Creating synchronous subscription id = %s to key = %r.', subscription_id, key)
		if subscription_id in self._subscriptions:
			callback = self._subscriptions[subscription_id].callback
		else:
			self._subscriptions[subscription_id] = Subscription(callback, None, None)
		self._hub.add_subscriber(key, callback)


	def async_subscribe(self, key: Key, subscription_id: str = DEFAULT_SUBSCRIPTION_ID) -> None:
		'''
		Subscribe to a particular `key`.
		A single queue is used for all subscriptions with this `subscription_id` (possibly to multiple keys).
		'''
		key = Key.create_from(key)
		self._logger.debug('Creating asynchronous subscription id = %s to key = %r.', subscription_id, key)
		if subscription_id in self._subscriptions:
			callback, queue, unread = self._subscriptions[subscription_id]
		else:
			queue = asyncio.Queue()
			callback = lambda key, message: queue.put_nowait((key, message))
			unread = queue.qsize
			self._subscriptions[subscription_id] = Subscription(callback, queue, unread)
		self._hub.add_subscriber(key, callback, unread)


	subscribe = async_subscribe
	'''
	Backward compatible method. Remove this in version 3.0.0.
	'''


	def unsubscribe(self, key: Key, subscription_id: str = DEFAULT_SUBSCRIPTION_ID) -> None:
		'''
		Unsubscribe from data previously subscribed to by `subscribe`.
		'''
		key = Key.create_from(key)
		self._logger.debug('Removing subscription id = %s to key = %r.', subscription_id, key)
		self._hub.remove_subscriber(
			key,
			self._subscriptions[subscription_id].callback,
			self._subscriptions[subscription_id].unread_callback
		)

	async def consume(self, subscription_id: str = DEFAULT_SUBSCRIPTION_ID) -> Tuple[Key, Any]:
		'''
		Consume a message from the subscription identified by `subscription_id`.

		:returns: (key, message)
		'''
		if not isinstance(self._subscriptions[subscription_id].queue, asyncio.Queue):
			raise ConsumeQueueNotFoundError('Cannot use consume to get data from sync subscriptions.')
		key, msg = await self._subscriptions[subscription_id].queue.get()
		return key, msg


	def add_sync_listener(self, keys: Union[Key, Set[Key]], callback: ListenerSyncCallback) -> None:
		'''
		Attach ``callback`` to one or more keys. Callback function is called directly after the publisher adds message
		for one of that keys.

		:param keys: One key, or a set of keys.
		'''
		keys, sub_id = self._get_listener_subscription(keys, callback)
		for key in keys:
			self.sync_subscribe(key, callback, sub_id)

		self._listeners[sub_id] = Listener(None, keys)


	def add_async_listener(self, keys: Union[Key, Set[Key]], callback: ListenerAsyncCallback) -> None:
		'''
		Attach asynchronous ``callback`` to one or more keys. If more than one key is provided, the callback will be
		reading all messages from one queue, and they are guaranteed to arrive in the same order they were published.

		:param keys: One key, or a set of keys.
		'''

		async def consumer() -> None:
			key, msg = await self.consume(sub_id)
			await callback(key, msg)

		keys, sub_id = self._get_listener_subscription(keys, callback)

		for key in keys:
			self.async_subscribe(key, sub_id)

		loop = aiopubsub.loop.Loop(consumer, delay = None) # type: ignore # See Key()
		loop.start()
		self._listeners[sub_id] = Listener(loop, keys)


	add_listener = add_sync_listener
	'''
	Backward compatible method. Remove this in version 3.0.0.
	'''


	async def remove_listener(self, keys: Union[Key, Set[Key]], callback: ListenerCallback) -> None:
		'''
		Both ``keys`` and ``callback`` must be exactly as passed to :meth:`add_listener` before
		so that the subscription can be found.

		:param callback: original Callback function
		:param keys: One key, or a set of keys.
		'''
		async with self._listeners_lock:
			keys, sub_id = self._get_listener_subscription(keys, callback)
			await self._stop_listener(sub_id, keys)
			del self._listeners[sub_id]


	async def remove_all_listeners(self) -> None:
		'''
		Remove all listeners from the subscriber.
		'''
		async with self._listeners_lock:
			for subscription_id, listener in self._listeners.items():
				await self._stop_listener(subscription_id, listener.keys)
			self._listeners.clear()


	async def _stop_listener(self, subscription_id: str, keys: Set[Key]) -> None:
		'''
		Unsubscribe and stop listener specified by ``subscription_id`` with ``keys``.
		'''
		for key in keys:
			self.unsubscribe(key, subscription_id)
		if self._listeners[subscription_id].loop is not None:
			await self._listeners[subscription_id].loop.stop_wait()


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
