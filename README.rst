aiopubsub - asyncio publish-subscribe within a process
======================================================

Simple pubsub pattern for asyncio applications.

Aiopubsub will use `logwood <https://github.com/qntln/logwood>`_ if it is installed, otherwise it will default
to the standard logging module.

Aiopubsub is only tested with Python 3.5. There are no plans to support older versions.


.. code-block:: python

	import asyncio
	import aiopubsub
	import logwood

	async def main():
		logwood.basic_config()
		hub = aiopubsub.Hub()
		publisher = aiopubsub.Publisher(hub, prefix = aiopubsub.Key('a'))
		subscriber = aiopubsub.Subscriber(hub, 'subscriber_id')

		def print_message(key, message):
			print(key, message)

		sub_key = aiopubsub.Key('a', 'b', '*')
		subscriber.subscribe(sub_key)
		subscriber.add_listener(sub_key, print_message)

		pub_key = aiopubsub.Key('b', 'c')
		publisher.publish(pub_key, 'Hello subscriber')
		await asyncio.sleep(0.001) # Let the callback fire.
		# "('a', 'b', 'c') Hello subscriber" will be printed.

		key, message = await subscriber.consume()
		assert key == aiopubsub.Key('a', 'b', 'c')
		assert message == 'Hello subscriber'

		subscriber.remove_all_listeners()


	asyncio.get_event_loop().run_until_complete(main())


Architecture
------------

**Hub** accepts messages from **Publishers** and routes them to **Subscribers**. Each message is routed by its
**Key** – an iterable of strings forming a hierarchic namespace. Subscribers may subscribe to wildcard keys,
where any part of the key is replaced with a :code:`*` (star).

A subscriber may consume messages through a coroutine-based API...

.. code-block:: python

	subscriber.subscribe(subscription_key)
	await subscriber.consume()

...or through a callback-based API.

.. code-block:: python

	subscriber.add_listener(subscription_key, callback_on_message)
