import asyncio

import pytest

import aiopubsub.testing.mocks

# pylint: disable=no-self-use, protected-access



class TestKey:
	def test_is_wildcard(self):
		assert not aiopubsub.Key('a').is_wildcard
		assert not aiopubsub.Key('a', 'b', 'c').is_wildcard
		assert aiopubsub.Key('*').is_wildcard
		assert aiopubsub.Key('a', 'b', '*').is_wildcard


	def test_is_wildcard_nonterminal(self):
		assert aiopubsub.Key('a', '*', 'c').is_wildcard
		assert aiopubsub.Key('*', 'b', 'c').is_wildcard
		assert aiopubsub.Key('*', 'b', '*').is_wildcard


	def test_is_subset_of(self):
		exact = aiopubsub.Key('a', 'b')
		assert not aiopubsub.Key('a').is_subset_of(exact)
		assert not aiopubsub.Key('a', 'x').is_subset_of(exact)
		assert aiopubsub.Key('a', 'b').is_subset_of(exact)

		wild = aiopubsub.Key('a', 'b', '*')
		assert not aiopubsub.Key('a').is_subset_of(wild)
		assert not aiopubsub.Key('a', 'x').is_subset_of(wild)
		#assert pubsub.Key('a', 'b').is_subset_of(wild)
		assert aiopubsub.Key('a', 'b', 'c').is_subset_of(wild)
		assert aiopubsub.Key('a', 'b', '*').is_subset_of(wild)
		assert aiopubsub.Key('a', 'b', 'c', 'd').is_subset_of(wild)
		assert aiopubsub.Key('a', 'b', 'c', 'd', '*').is_subset_of(wild)


	def test_is_subset_of_wildcard_in_the_middle(self):
		wild1 = aiopubsub.Key('a', 'b', '*', 'd')
		assert not aiopubsub.Key('a', 'b', 'c').is_subset_of(wild1)
		assert not aiopubsub.Key('a', 'b', 'c', 'd', 'e').is_subset_of(wild1)
		assert not aiopubsub.Key('a', 'b', 'c', 'x').is_subset_of(wild1)
		assert aiopubsub.Key('a', 'b', 'c', 'd').is_subset_of(wild1)
		assert aiopubsub.Key('a', 'b', '*', 'd').is_subset_of(wild1)


	def test_add(self):
		added = aiopubsub.Key('1') + aiopubsub.Key('2')
		assert added == aiopubsub.Key('1', '2')
		assert isinstance(added, aiopubsub.Key)


	def test_create_from(self):
		assert aiopubsub.Key.create_from(('a', 'b')) == aiopubsub.Key('a', 'b')
		assert aiopubsub.Key.create_from(['a', 'b']) == aiopubsub.Key('a', 'b')
		assert aiopubsub.Key.create_from('ab') == aiopubsub.Key('ab')



class TestHub:

	@pytest.fixture
	def hub(self):
		return aiopubsub.Hub()


	@pytest.fixture
	def data(self):
		return []


	@pytest.fixture
	def callback(self, data):
		return lambda key, message: data.append((key, message))


	def test_add_subscriber(self, hub, data, callback):
		'''
		Subscriber receives messages after being added.
		'''
		k = aiopubsub.Key('a', 'b')
		hub.add_subscriber(k, callback)
		hub.publish(k, 'message1')
		assert data == [(k, 'message1')]


	def test_remove_subscriber(self, hub, data, callback):
		'''
		No message is received after the subscriber is removed.
		'''
		k = aiopubsub.Key('a', 'b')
		hub.add_subscriber(k, callback)
		hub.publish(k, 'message1')
		hub.remove_subscriber(k, callback)
		hub.publish(k, 'message2')
		assert data == [(k, 'message1')]


	def test_has_subscribers(self, hub, callback):
		k = aiopubsub.Key('a', 'b')
		# No subs at first
		assert not hub.has_subscribers()
		assert not hub.has_subscribers(k)
		# Add one subscriber
		hub.add_subscriber(k, callback)
		assert hub.has_subscribers()
		assert hub.has_subscribers(k)
		# No subs for a different key
		assert not hub.has_subscribers(aiopubsub.Key('a', 'x'))
		# Remove the subscriber => no subs again.
		hub.remove_subscriber(k, callback)
		assert not hub.has_subscribers()
		assert not hub.has_subscribers(k)


	def test_publish_multi(self, hub):
		'''
		Multiple subscribers to the same key receive the same message.
		'''
		data1 = []
		data2 = []
		def callback1(k, m):
			data1.append((k, m))
		def callback2(k, m):
			data2.append((k, m))
		k = aiopubsub.Key('a', 'b')
		hub.add_subscriber(k, callback1)
		hub.add_subscriber(k, callback2)
		hub.publish(k, 'message')
		assert data1 == [(k, 'message')]
		assert data2 == [(k, 'message')]


	def test_publish_wildcard(self, hub, data, callback):
		k1 = aiopubsub.Key('a', 'b')
		k2 = aiopubsub.Key('a', 'x')
		hub.add_subscriber(aiopubsub.Key('a', '*'), callback)
		hub.publish(k1, 'message')
		assert data == [(k1, 'message')]
		hub.publish(k2, 'message2')
		assert data == [(k1, 'message'), (k2, 'message2')]
		# Different key
		hub.publish(aiopubsub.Key('b', 'b'), 'message3')
		assert data == [(k1, 'message'), (k2, 'message2')]


	def test_cache_invalidation_wildcard(self, hub):
		data1 = []
		data2 = []
		def callback1(k, m):
			data1.append((k, m))
		def callback2(k, m):
			data2.append((k, m))
		k = aiopubsub.Key('a', 'b')
		hub.add_subscriber(aiopubsub.Key('*', 'b'), callback1)
		hub.publish(k, 'message')
		hub.add_subscriber(aiopubsub.Key('a', 'b'), callback2)
		hub.publish(k, 'message2')
		assert data1 == [(k, 'message'), (k, 'message2')]
		assert data2 == [(k, 'message2')]


	# TODO: test Hub status messages (addedSubscriber etc.)



class TestPublisher:

	@pytest.fixture
	def hub(self):
		return aiopubsub.testing.mocks.MockHub()


	@pytest.fixture
	def publisher(self, hub):
		return aiopubsub.Publisher(hub, 'Tester')


	def test_publish(self, hub, publisher):
		'''
		Publisher ID must be prepended to the key.
		'''
		publisher.publish(('a', 'b'), 'message')
		assert hub.published == [(aiopubsub.Key('Tester', 'a', 'b'), 'message')]


	def test_has_subscribers(self, hub, publisher):
		k1 = aiopubsub.Key('a', 'b')
		k2 = aiopubsub.Key('a', 'x')
		assert not publisher.has_subscribers()
		assert not publisher.has_subscribers(k1)
		assert not publisher.has_subscribers(k2)
		hub.subscribers[aiopubsub.Key('Tester') + k1].append(True)
		assert publisher.has_subscribers()
		assert publisher.has_subscribers(k1)
		assert not publisher.has_subscribers(k2)



class TestSubscriber:

	@pytest.fixture
	def hub(self):
		return aiopubsub.Hub()


	@pytest.fixture
	def subscriber(self, hub):
		return aiopubsub.Subscriber(hub, 'Tester')


	@pytest.fixture
	def data(self):
		return []


	@pytest.fixture(params = ['sync', 'async'])
	def callback(self, data, request):
		if request.param == 'sync':
			def callback(k, m):
				data.append((k, m))
		else:
			async def callback(k, m):
				data.append((k, m))
		return callback


	def test_subscribe_unsubscribe(self, hub, subscriber):
		k = aiopubsub.Key('a', 'b')
		subscriber.async_subscribe(k)
		assert len(hub._subscribers) == 1
		assert len(hub._subscribers[k]) == 1
		subscriber.unsubscribe(k)
		assert not hub._subscribers[k]


	@pytest.mark.asyncio
	async def test_consume(self, hub, subscriber):
		k = aiopubsub.Key('a', 'b')
		subscriber.async_subscribe(k)
		hub.publish(k, 'message1')
		hub.publish(k, 'message2')
		consumed = await subscriber.consume()
		assert consumed == (k, 'message1')
		consumed = await subscriber.consume()
		assert consumed == (k, 'message2')


	@pytest.mark.asyncio
	async def test_add_listener_remove_listener(self, hub, subscriber, callback, data):
		k = aiopubsub.Key('a', 'b')
		if asyncio.iscoroutinefunction(callback):
			subscriber.add_async_listener(k, callback)
		else:
			subscriber.add_sync_listener(k, callback)
		# Callback must be fired on every message.
		hub.publish(k, 'message1')
		await asyncio.sleep(0.001)
		assert data == [(k, 'message1')]
		hub.publish(k, 'message2')
		await asyncio.sleep(0.001)
		assert data, [(k, 'message1') == (k, 'message2')]
		# No more firings after the listener is removed.
		await subscriber.remove_listener(k, callback)
		hub.publish(k, 'message3')
		await asyncio.sleep(0.01)
		assert data == [(k, 'message1'), (k, 'message2')]


	@pytest.mark.asyncio
	async def test_add_listener_remove_listener_multiple_keys(self, hub, subscriber, callback, data):
		k1 = aiopubsub.Key('a', 'b')
		k2 = aiopubsub.Key('a', 'x')
		if asyncio.iscoroutinefunction(callback):
			subscriber.add_async_listener({k1, k2}, callback)
		else:
			subscriber.add_sync_listener({k1, k2}, callback)
		# In asynchronous case both keys must be read by the same callback
		assert hub._subscribers[k1][0] is hub._subscribers[k2][0]
		# Callback must be fired on every message.
		hub.publish(k1, 'message1')
		await asyncio.sleep(0.001)
		assert data == [(k1, 'message1')]
		hub.publish(k2, 'message2')
		await asyncio.sleep(0.001)
		assert data == [(k1, 'message1'), (k2, 'message2')]
		# No more firings after the listener is removed.
		await subscriber.remove_listener({k1, k2}, callback)
		hub.publish(k1, 'message3')
		await asyncio.sleep(0.01)
		assert data == [(k1, 'message1'), (k2, 'message2')]
		hub.publish(k2, 'message4')
		await asyncio.sleep(0.01)
		assert data == [(k1, 'message1'), (k2, 'message2')]
		# Queues are removed.
		assert hub._subscribers[k1] == []
		assert hub._subscribers[k2] == []


	@pytest.mark.asyncio
	async def test_remove_all_listeners(self, subscriber):
		data = []
		def sync_callback(k, m):
			data.append((k, m))
		async def async_callback(k, m):
			data.append((k, m))
		# nothing happens when listeners are empty
		assert not bool(subscriber._listeners)
		await subscriber.remove_all_listeners()
		assert not bool(subscriber._listeners)
		# removes all present sync listeners
		k1 = aiopubsub.Key('a', 'b')
		k2 = aiopubsub.Key('a', 'x')
		subscriber.add_sync_listener(k1, sync_callback)
		subscriber.add_sync_listener(k2, sync_callback)
		assert len(subscriber._listeners) == 2
		await subscriber.remove_all_listeners()
		assert not bool(subscriber._listeners)
		# removes all present listeners
		k1 = aiopubsub.Key('a', 'b')
		k2 = aiopubsub.Key('a', 'x')
		subscriber.add_sync_listener(k1, sync_callback)
		subscriber.add_async_listener(k2, async_callback)
		assert len(subscriber._listeners) == 2
		await subscriber.remove_all_listeners()
		assert not bool(subscriber._listeners)



@pytest.mark.asyncio
async def test_get_queue_size():
	testcase = {
		('a',): 1,
		('a', 'b',): 2,
		('a', 'b', 'c',): 3,
		('x', 'b',): 8,
		('x', 'c',): 12,
		('x',): 16,
	}

	hub = aiopubsub.Hub()
	subscriber = aiopubsub.Subscriber(hub, 'Tester')
	data = []
	async def callback(k, m):
		data.append((k, m))

	for key, messages_count in testcase.items():
		k = aiopubsub.Key(*key)
		subscriber.add_async_listener(k, callback)
		subscriber._get_listener_subscription(key, callback)

		for _ in range(messages_count):
			hub.publish(k, 'message')

		#Immediately after publishing queues should be full.
		assert sum(hub.get_subscriber_queue_sizes(k)) == messages_count
		await asyncio.sleep(0.01)
		#After time messages are consumed.
		assert sum(hub.get_subscriber_queue_sizes(k)) == 0



@pytest.mark.asyncio
async def test_sync_and_async_listeners():
	hub = aiopubsub.Hub()
	subscriber = aiopubsub.Subscriber(hub, 'Tester')

	data_sync = []
	data_async = []
	def sync_callback(k, m):
		data_sync.append((k, m))
	async def async_callback(k, m):
		data_async.append((k, m))
	k = aiopubsub.Key('a', 'b')
	subscriber.add_sync_listener(k, sync_callback)
	subscriber.add_async_listener(k, async_callback)

	hub.publish(k, 'message1')
	await asyncio.sleep(0.001)
	assert data_sync == [(k, 'message1')]
	assert data_async == [(k, 'message1')]
	hub.publish(k, 'message2')
	await asyncio.sleep(0.001)
	assert data_sync, [(k, 'message1'), (k, 'message2')]
	assert data_async, [(k, 'message1'), (k, 'message2')]
	## No more firings after the listener is removed.
	await subscriber.remove_listener(k, sync_callback)
	await subscriber.remove_listener(k, async_callback)
	hub.publish(k, 'message3')
	await asyncio.sleep(0.01)
	assert data_sync == [(k, 'message1'), (k, 'message2')]
	assert data_async == [(k, 'message1'), (k, 'message2')]



@pytest.mark.asyncio
async def test_remove_listeners_race_condition():
	hub = aiopubsub.Hub()
	subscriber = aiopubsub.Subscriber(hub, 'Tester')

	for idx in range(10):
		subscriber.add_sync_listener(aiopubsub.Key('a', idx), lambda *_: None)

	future = asyncio.ensure_future(subscriber.remove_all_listeners())
	future2 = asyncio.ensure_future(subscriber.remove_all_listeners())
	await asyncio.wait([future, future2])
