from typing import List, Tuple

import bz2
import json
import os

import logwood.handlers.stderr
import pytest

import aiopubsub



handlers = [logwood.handlers.stderr.ColoredStderrHandler()]
logwood.basic_config(handlers)



@pytest.fixture
def hub():
	return aiopubsub.Hub()


@pytest.fixture
def callback():
	return lambda key, message: None


@pytest.fixture(scope = 'module')
def real_recorded_published_keys() -> List[Tuple[aiopubsub.Key, aiopubsub.Key, bool]]:
	'''
	Load a dump of keys published in a real application in production. The keys have been anonymised.
	'''
	path = os.path.join(os.path.dirname(__file__), 'assets', 'real_recorded_published_keys.json.bz2')
	with bz2.open(path) as f:
		data = [
			(
				aiopubsub.Key.create_from(published_key),
				aiopubsub.Key.create_from(subscriber_key),
				should_match
			)
			for published_key, subscriber_key, should_match in json.load(f)
		]
	return data



@pytest.mark.parametrize('matching_keys', [1.0, 0.5, 0.1])
@pytest.mark.parametrize('subscribers_count', [1, 10, 100, 1000])
def test_hub_publish_match_exact(hub, callback, benchmark, subscribers_count, matching_keys):
	inv_matching_keys = int(1 / matching_keys)
	for i in range(subscribers_count):
		if i % inv_matching_keys == 0:
			k = aiopubsub.Key('a', 'b')
		else:
			k = aiopubsub.Key('c', 'd')
		hub.add_subscriber(k, callback)

	k = aiopubsub.Key('a', 'b')
	benchmark(hub.publish, k, 'message')



@pytest.mark.parametrize('matching_keys', [1.0, 0.5, 0.1])
@pytest.mark.parametrize('subscribers_count', [1, 10, 100, 1000])
def test_hub_publish_match_wildcard(hub, callback, benchmark, subscribers_count, matching_keys):
	inv_matching_keys = int(1 / matching_keys)
	for i in range(subscribers_count):
		if i % inv_matching_keys == 0:
			k = aiopubsub.Key('a', '*')
		else:
			k = aiopubsub.Key('c', '*')
		hub.add_subscriber(k, callback)

	k = aiopubsub.Key('a', 'b')
	benchmark(hub.publish, k, 'message')



def test_is_subset_of(benchmark, real_recorded_published_keys):
	def test():
		for published_key, subscriber_key, should_match in real_recorded_published_keys:
			assert published_key.is_subset_of(subscriber_key) == should_match
	benchmark(test)



@pytest.mark.parametrize('messages_count', [10, 1000, 100000])
@pytest.mark.parametrize('subscribers_count', [1, 100, 1000])
def test_hub_publish_real_keys(hub, callback, benchmark, real_recorded_published_keys, messages_count, subscribers_count):
	def test():
		for published_key, _, _ in real_recorded_published_keys[:messages_count]:
			hub.publish(published_key, 'message')

	# Create subscribers for up to `subscriber_count` subscriber keys
	subscribed = set()
	for _, subscriber_key, _ in real_recorded_published_keys:
		if subscriber_key not in subscribed:  # has_subscriber() is slow when cache has not been built yet
			hub.add_subscriber(subscriber_key, callback)
			subscribed.add(subscriber_key)
			subscribers_count -= 1
			if subscribers_count <= 0:
				break

	benchmark(test)
