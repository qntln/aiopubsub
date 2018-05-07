aiopubsub - asyncio publish-subscribe within a process
======================================================

Simple pubsub pattern for asyncio applications.

aiopubsub is only tested with Python 3.6. There are no plans to support older versions.


.. code-block:: python

	import asyncio
	import aiopubsub
	import logwood

	async def main():
		logwood.basic_config()
		hub = aiopubsub.Hub()
		publisher = aiopubsub.Publisher(hub, prefix = aiopubsub.Key('a'))
		subscriber = aiopubsub.Subscriber(hub, 'subscriber_id')

		sub_key = aiopubsub.Key('a', 'b', '*')
		subscriber.subscribe(sub_key)

		pub_key = aiopubsub.Key('b', 'c')
		publisher.publish(pub_key, 'Hello subscriber')
		await asyncio.sleep(0.001) # Let the callback fire.
		# "('a', 'b', 'c') Hello subscriber" will be printed.

		key, message = await subscriber.consume()
		assert key == aiopubsub.Key('a', 'b', 'c')
		assert message == 'Hello subscriber'

		subscriber.remove_all_listeners()


	asyncio.get_event_loop().run_until_complete(main())

or, instead of directly subscribing to the key, we can create a listener that will call a synchronous callback
when a new message arrives.

.. code-block:: python

	def print_message(key, message):
		print(key, message)

	subscriber.add_sync_listener(sub_key, print_message)

Or, if we have a coroutine callback we can create an asynchronous listener:

.. code-block:: python

	async def print_message(key, message):
		await asyncio.sleep(1)
		print(key, message)

	subscriber.add_async_listener(sub_key, print_message)


Aiopubsub will use `logwood <https://github.com/qntln/logwood>`_ if it is installed, otherwise it will default
to the standard logging module. Note that ``logwood`` is required to run tests.


Architecture
------------

**Hub** accepts messages from **Publishers** and routes them to **Subscribers**. Each message is routed by its
**Key** - an iterable of strings forming a hierarchic namespace. Subscribers may subscribe to wildcard keys,
where any part of the key may be replaced replaced with a ``*`` (star).

``addedSubscriber`` and ``removedSubscriber`` messages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a new subscriber is added the Hub sends this message

.. code-block::

	{
		"key": ("key", "of", "added", "subscriber"),
		"currentSubscriberCount": 2
	}

under the key ``('Hub', 'addedSubscriber', 'key', 'of', 'added', 'subscriber')`` (the part after ``addedSubscriber``
is made of the subscribed key). Note the ``currentSubscriberCount`` field indicating how many subscribers are currently
subscribed.

When a subscriber is removed a message in the same format is sent, but under the key
``('Hub', 'removedSubscriber', 'key', 'of', 'added', 'subscriber')``.
