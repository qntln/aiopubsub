from typing import Optional, Awaitable

import asyncio
import contextlib

from aiopubsub.logging_compat import get_logger



class Loop:
	'''
	Run a coroutine in a loop with a delay after every iteration.
	The loop can be conveniently started, stopped, and restarted.
	'''

	def __init__(self, coro: Awaitable, delay: Optional[float]) -> None:
		self.coro = coro
		self.task = None # type: asyncio.Task
		self.delay = delay
		self.name = repr(coro)
		if hasattr(coro, '__name__'):
			self.name = '[{}, {}]'.format(coro.__name__, coro) # type: ignore

		self._is_running = asyncio.Event()
		self._logger = get_logger(self.__class__.__name__)


	def start(self) -> None:
		if not self._is_running.is_set():
			self._is_running.set()
			asyncio.ensure_future(self._run())


	def stop(self) -> None:
		self._is_running.clear()
		if self.task is not None:
			self.task.cancel()
			self.task = None


	async def stop_wait(self) -> None:
		self._is_running.clear()
		if self.task is not None:
			self.task.cancel()
			with contextlib.suppress(asyncio.CancelledError):
				await self.task
			self.task = None


	@property
	def is_running(self) -> bool:
		return self._is_running.is_set()


	async def _run(self):
		while self._is_running.is_set():
			self.task = asyncio.ensure_future(self.coro())
			try:
				await self.task
			except asyncio.CancelledError:
				if self._is_running.is_set():
					self._logger.exception('Unhandled CancelledError in = %s', self.name)
					raise
				else:
					self._logger.debug('Stopping task = %s', self.name)
					break
			except:
				self._logger.exception('Uncaught exception in _run in coroutine = %s', self.name)
				self._is_running.clear()
				raise
			if self.delay is not None:
				await asyncio.sleep(self.delay)
