from typing import Any, Iterable, Callable, Set # NOQA

import asyncio
import aiopubsub # NOQA

from . import Hub, Publisher, PossibleKey



class ThrottledPublisher:
	'''
	This class wraps around :class:`Publisher` and waits after calling :meth:`publish` until the queue sizes
	of all subscribers consuming from this publisher drop below the specified max_qsize (``max_qsize``).
	An appropriate sequence of wait times should be provided by the ``delay_sequence_factory`` function
	every time it is called.
	'''

	def __init__(self, hub: Hub, publisher: Publisher, max_qsize: int,
	delay_sequence_factory: Callable[[], Iterable[float]]) -> None:
		self._hub = hub
		self._publisher = publisher
		self._max_qsize = max_qsize
		self._published_keys = set() # type: Set[aiopubsub.Key]
		self._delay_sequence_factory = delay_sequence_factory


	async def publish(self, key: PossibleKey, message: Any) -> None:
		self._publisher.publish(key, message)

		self._published_keys.add(self._publisher.prefix + key)

		delay_generator = iter(self._delay_sequence_factory())
		for published_key in self._published_keys:
			while True:
				queue_sizes = self._hub.get_subscriber_queue_sizes(published_key)
				if not queue_sizes or max(queue_sizes) <= self._max_qsize:
					break
				await asyncio.sleep(next(delay_generator))
