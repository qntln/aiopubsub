import pytest

import aiopubsub
import aiopubsub.testing.mocks
import logwood.handlers.stderr


handlers = [logwood.handlers.stderr.ColoredStderrHandler()]
logwood.basic_config(handlers)



@pytest.fixture
def hub():
	return aiopubsub.Hub()



@pytest.mark.parametrize('matching_keys', [1.0, 0.5, 0.1])
@pytest.mark.parametrize('subscribers_count', [1, 10, 100, 1000])
def test_hub_publish_match_exact(hub, benchmark, subscribers_count, matching_keys):
	inv_matching_keys = int(1 / matching_keys)
	for i in range(subscribers_count):
		if i % inv_matching_keys == 0:
			k = aiopubsub.Key('a', 'b')
		else:
			k = aiopubsub.Key('c', 'd')
		q = aiopubsub.testing.mocks.MockAsyncQueue()
		hub.add_subscriber(k, q)

	k = aiopubsub.Key('a', 'b')
	benchmark(hub.publish, k, 'message')



@pytest.mark.parametrize('matching_keys', [1.0, 0.5, 0.1])
@pytest.mark.parametrize('subscribers_count', [1, 10, 100, 1000])
def test_hub_publish_match_wildcard(hub, benchmark, subscribers_count, matching_keys):
	inv_matching_keys = int(1 / matching_keys)
	for i in range(subscribers_count):
		if i % inv_matching_keys == 0:
			k = aiopubsub.Key('a', '*')
		else:
			k = aiopubsub.Key('c', '*')
		q = aiopubsub.testing.mocks.MockAsyncQueue()
		hub.add_subscriber(k, q)

	k = aiopubsub.Key('a', 'b')
	benchmark(hub.publish, k, 'message')
