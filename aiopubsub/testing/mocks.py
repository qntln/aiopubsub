import collections


class MockHub:
	def __init__(self):
		self.published = []
		self.published_by_key = collections.defaultdict(list)
		self.subscribers = collections.defaultdict(list)

	def publish(self, k, m):
		self.published.append((k, m))
		self.published_by_key[k].append(m)

	# pylint: disable=unused-argument
	def add_subscriber(self, k, q, unread = None):
		self.subscribers[k].append(q)

	# pylint: disable=unused-argument
	def remove_subscriber(self, k, q, unread = None):
		self.subscribers[k].remove(q)

	def has_subscribers(self, k):
		return any(s.is_subset_of(k) and qs for s, qs in self.subscribers.items())

	def get_subscriber_queue_sizes(self, k):
		return (len(self.published_by_key[k]),)
