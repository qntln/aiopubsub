from typing import Optional

import asyncio



class Loop:
	'''
	Run a coroutine in a loop with a delay after every iteration.
	The loop can be conveniently started, stopped, and restarted.
	'''

	def __init__(self, coro, delay: Optional[float]) -> None:
		self.coro = coro
		self.task = None # type: asyncio.Task
		self.delay = delay
		self._is_running = asyncio.Event()


	def start(self):
		if not self._is_running.is_set():
			self._is_running.set()
			asyncio.ensure_future(self._run())


	def stop(self):
		self._is_running.clear()
		if self.task:
			self.task.cancel()
			self.task = None


	@property
	def is_running(self) -> bool:
		return self._is_running.is_set()


	async def _run(self):
		while self._is_running.is_set():
			self.task = asyncio.ensure_future(self.coro())
			await asyncio.wait_for(self.task, timeout = None)
			if self.delay is not None:
				await asyncio.sleep(self.delay)
