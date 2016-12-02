import asyncio
import pytest

import aiopubsub.testing.mocks



class Test01Key:
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



class Test02Hub:

	@pytest.fixture
	def hub(self):
		return aiopubsub.Hub()

	def test_01_add_subscriber(self, hub):
		'''
		Subscriber receives messages after being added.
		'''
		k = aiopubsub.Key('a', 'b')
		q = aiopubsub.testing.mocks.MockAsyncQueue()
		hub.add_subscriber(k, q)
		hub.publish(k, 'message')
		assert q.data == [(k, 'message')]


	def test_02_remove_subscriber(self, hub):
		'''
		No message is received after the subscriber is removed.
		'''
		k = aiopubsub.Key('a', 'b')
		q = aiopubsub.testing.mocks.MockAsyncQueue()
		hub.add_subscriber(k, q)
		hub.remove_subscriber(k, q)
		hub.publish(k, 'message')
		assert q.data == []


	def test_03_has_subscribers(self, hub):
		k = aiopubsub.Key('a', 'b')
		q = aiopubsub.testing.mocks.MockAsyncQueue()
		# No subs at first
		assert not hub.has_subscribers()
		assert not hub.has_subscribers(k)
		# Add one subscriber
		hub.add_subscriber(k, q)
		assert hub.has_subscribers()
		assert hub.has_subscribers(k)
		# No subs for a different key
		assert not hub.has_subscribers(aiopubsub.Key('a', 'x'))
		# Remove the subscriber => no subs again.
		hub.remove_subscriber(k, q)
		assert not hub.has_subscribers()
		assert not hub.has_subscribers(k)


	def test_04_publish_multi(self, hub):
		'''
		Multiple subscribers to the same key receive the same message.
		'''
		k = aiopubsub.Key('a', 'b')
		q1 = aiopubsub.testing.mocks.MockAsyncQueue()
		q2 = aiopubsub.testing.mocks.MockAsyncQueue()
		hub.add_subscriber(k, q1)
		hub.add_subscriber(k, q2)
		hub.publish(k, 'message')
		assert q1.data == [(k, 'message')]
		assert q2.data == [(k, 'message')]


	def test_04_publish_wildcard(self, hub):
		q = aiopubsub.testing.mocks.MockAsyncQueue()
		k1 = aiopubsub.Key('a', 'b')
		k2 = aiopubsub.Key('a', 'x')
		hub.add_subscriber(aiopubsub.Key('a', '*'), q)
		hub.publish(k1, 'message')
		assert q.data == [(k1, 'message')]
		hub.publish(k2, 'message2')
		assert q.data == [(k1, 'message'), (k2, 'message2')]
		# Different key
		hub.publish(aiopubsub.Key('b', 'b'), 'message3')
		assert q.data == [(k1, 'message'), (k2, 'message2')]


	# TODO: test Hub status messages (addedSubscriber etc.)



class Test03Publisher:

	@pytest.fixture
	def hub(self):
		return aiopubsub.testing.mocks.MockHub()


	@pytest.fixture
	def publisher(self, hub):
		return aiopubsub.Publisher(hub, 'Tester')


	def test_01_publish(self, hub, publisher):
		'''
		Publisher ID must be prepended to the key.
		'''
		publisher.publish(('a', 'b'), 'message')
		assert hub.published == [(aiopubsub.Key('Tester', 'a', 'b'), 'message')]


	def test_02_has_subscribers(self, hub, publisher):
		k1 = aiopubsub.Key('a', 'b')
		k2 = aiopubsub.Key('a', 'x')
		assert not publisher.has_subscribers()
		assert not publisher.has_subscribers(k1)
		assert not publisher.has_subscribers(k2)
		hub.subscribers[aiopubsub.Key('Tester') + k1].append(True)
		assert publisher.has_subscribers()
		assert publisher.has_subscribers(k1)
		assert not publisher.has_subscribers(k2)



class Test04Subscriber:

	@pytest.fixture
	def hub(self):
		return aiopubsub.testing.mocks.MockHub()


	@pytest.fixture
	def subscriber(self, hub):
		return aiopubsub.Subscriber(hub, 'Tester')


	def test_01_subscribe_unsubscribe(self, hub, subscriber):
		k = aiopubsub.Key('a', 'b')
		subscriber.subscribe(k)
		assert len(hub.subscribers) == 1
		assert len(hub.subscribers[k]) == 1
		subscriber.unsubscribe(k)
		assert not hub.subscribers[k]


	@pytest.mark.asyncio
	async def test_02_consume(self, hub, subscriber):
		k = aiopubsub.Key('a', 'b')
		subscriber.subscribe(k)
		q = hub.subscribers[k][0]
		q.put_nowait((k, 'message1'))
		q.put_nowait((k, 'message2'))
		consumed = await subscriber.consume()
		assert consumed == (k, 'message1')
		consumed = await subscriber.consume()
		assert consumed == (k, 'message2')


	@pytest.mark.asyncio
	async def test_03_add_listener_remove_listener(self, hub, subscriber):
		data = []
		def callback(k, m):
			data.append((k, m))
		k = aiopubsub.Key('a', 'b')
		subscriber.add_listener(k, callback)
		q = hub.subscribers[k][0]
		# Callback must be fired on every message.
		q.put_nowait((k, 'message1'))
		await asyncio.sleep(0.001)
		assert data == [(k, 'message1')]
		q.put_nowait((k, 'message2'))
		await asyncio.sleep(0.001)
		assert data, [(k, 'message1') == (k, 'message2')]
		# No more firings after the listener is removed.
		subscriber.remove_listener(k, callback)
		q.put_nowait((k, 'message3'))
		await asyncio.sleep(0.01)
		assert data == [(k, 'message1'), (k, 'message2')]


	@pytest.mark.asyncio
	async def test_04_add_listener_remove_listener_multiple_keys(self, hub, subscriber):
		data = []
		def callback(k, m):
			data.append((k, m))
		k1 = aiopubsub.Key('a', 'b')
		k2 = aiopubsub.Key('a', 'x')
		subscriber.add_listener({k1, k2}, callback)
		# Both keys must be read by the same queue.
		assert hub.subscribers[k1][0] is hub.subscribers[k2][0]
		q = hub.subscribers[k1][0]
		# Callback must be fired on every message.
		q.put_nowait((k1, 'message1'))
		await asyncio.sleep(0.001)
		assert data == [(k1, 'message1')]
		q.put_nowait((k2, 'message2'))
		await asyncio.sleep(0.001)
		assert data == [(k1, 'message1'), (k2, 'message2')]
		# No more firings after the listener is removed.
		subscriber.remove_listener({k1, k2}, callback)
		q.put_nowait((k1, 'message3'))
		await asyncio.sleep(0.01)
		assert data == [(k1, 'message1'), (k2, 'message2')]
		q.put_nowait((k2, 'message4'))
		await asyncio.sleep(0.01)
		assert data == [(k1, 'message1'), (k2, 'message2')]
		# Queues are removed.
		assert hub.subscribers[k1] == []
		assert hub.subscribers[k2] == []

	@pytest.mark.asyncio
	async def test_05_remove_all_listeners(self, subscriber):
		data = []
		def callback(k, m):
			data.append((k, m))
		# nothing happens when listeners are empty
		assert len(subscriber._listeners) == 0
		subscriber.remove_all_listeners()
		assert len(subscriber._listeners) == 0
		# removes all present listeners
		k1 = aiopubsub.Key('a', 'b')
		k2 = aiopubsub.Key('a', 'x')
		subscriber.add_listener(k1, callback)
		subscriber.add_listener(k2, callback)
		assert len(subscriber._listeners) == 2
		subscriber.remove_all_listeners()
		assert len(subscriber._listeners) == 0
