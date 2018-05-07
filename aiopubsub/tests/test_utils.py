import asyncio
import pytest

import aiopubsub
import aiopubsub.utils
import aiopubsub.testing.mocks



def simple_delay():
	while True:
		yield 0.01


@pytest.fixture
def hub():
	return aiopubsub.Hub()


@pytest.fixture
def publisher(hub):
	return aiopubsub.Publisher(hub, 'Tester')


@pytest.fixture
def subscriber(hub):
	return aiopubsub.Subscriber(hub, 'Tester')


@pytest.fixture
def throttled_publisher(hub, publisher):
	return aiopubsub.utils.ThrottledPublisher(hub, publisher, 2, simple_delay)


@pytest.mark.asyncio
async def test_publish_empty(throttled_publisher):
	k = aiopubsub.Key('a', 'b')
	await throttled_publisher.publish(k, 'message')


@pytest.mark.asyncio
async def test_publish(publisher, throttled_publisher, subscriber):
	k = aiopubsub.Key('a', 'b')
	subscriber.subscribe(aiopubsub.Key('Tester', *k))

	await throttled_publisher.publish(k, 'message')
	assert publisher.has_subscribers(k)
	_, message = await subscriber.consume()
	assert message == 'message'


@pytest.mark.asyncio
async def test_publish_delay(throttled_publisher, subscriber):
	k = aiopubsub.Key('a', 'b')
	subscriber.subscribe(aiopubsub.Key('Tester', *k))

	await throttled_publisher.publish(k, 'message')
	await throttled_publisher.publish(k, 'message')

	async def consume():
		await asyncio.sleep(0.01)
		await subscriber.consume()

	async def block_publish():
		await throttled_publisher.publish(k, 'message')

	publish_task = asyncio.ensure_future(block_publish())
	await asyncio.sleep(0.02)
	assert not publish_task.done()
	consume_task = asyncio.ensure_future(consume())
	await consume_task
	await asyncio.sleep(0.02)
	done, _ = await asyncio.wait([publish_task], timeout = 0.5)
	assert publish_task in done
