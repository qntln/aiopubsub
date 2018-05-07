import asyncio
import contextlib
import time
import pytest
import asynctest

import aiopubsub.loop


@pytest.fixture
def calls():
	return []


@pytest.fixture
def callback(calls):
	async def callback_function():
		await asyncio.sleep(0.050)
		calls.append(time.time())
	return callback_function


@pytest.mark.asyncio
async def test_delay(calls, callback):
	loop = aiopubsub.loop.Loop(callback, delay = 0.005)
	loop.start()
	for i in range(3):
		await asyncio.sleep(0.060)
		assert len(calls) == i + 1
	loop.stop()


@pytest.mark.asyncio
async def test_cancel(calls, callback):
	'''
	Cancel the callback when the loop is stopped mid-callback.
	'''
	loop = aiopubsub.loop.Loop(callback, None)
	loop.start()
	await asyncio.sleep(0.055)
	assert len(calls) == 1
	# Stop the loop during the callback's 50ms wait.
	await asyncio.sleep(0.010)
	loop.stop()
	await asyncio.sleep(0.100)
	assert len(calls) == 1


@pytest.mark.asyncio
async def test_startstop(calls, callback):
	'''
	Start & stop the loop repeatedly.
	'''
	loop = aiopubsub.loop.Loop(callback, None)
	# First call.
	loop.start()
	await asyncio.sleep(0.055)
	assert len(calls) == 1
	# Stop and check no more calls happen.
	loop.stop()
	await asyncio.sleep(0.055)
	assert len(calls) == 1
	# Restart and let two more calls happen.
	loop.start()
	await asyncio.sleep(0.110)
	assert len(calls) == 3
	loop.stop()


@pytest.mark.asyncio
async def test_loop_raises(logwood_handler_mock):
	'''
	Test that loop logs if the coroutine raises
	'''
	mock_coro = asynctest.CoroutineMock(side_effect = [RuntimeError('Just for test')])
	loop = aiopubsub.loop.Loop(mock_coro, None)

	with contextlib.suppress(RuntimeError):
		loop.start()
		await asyncio.sleep(0.055)
		assert mock_coro.call_count == 1
		assert any(msg.startswith('Uncaught exception in _run') for msg in logwood_handler_mock['ERROR'])
		loop.stop()
