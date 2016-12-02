import collections



class MockAsyncQueue:
	def __init__(self):
		self.data = []

	async def put(self, x):
		self.data.append(x)

	def put_nowait(self, x):
		self.data.append(x)

	async def get(self):
		return self.data.pop(0)


class MockHub:
	def __init__(self):
		self.published = []
		self.subscribers = collections.defaultdict(list)

	def publish(self, k, m):
		self.published.append((k, m))

	def add_subscriber(self, k, q):
		self.subscribers[k].append(q)

	def remove_subscriber(self, k, q):
		self.subscribers[k].remove(q)

	def has_subscribers(self, k):
		return any(s.is_subset_of(k) and qs for s, qs in self.subscribers.items())
