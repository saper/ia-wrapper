import sys
from functools import partial
try:
    import ujson as json
except ImportError:
    import json

import trollius as asyncio
from trollius import From
from internetarchive import get_item
from internetarchive.session import ArchiveSession


# Mine class
# ________________________________________________________________________________________
class Mine(object):
    """This class is for concurrently retrieving
    :class:`internetarchive.Item <Item>` objects.

    Usage::

        >>> from internetarchive.mine import Mine
        >>> identifiers = [x.strip() for x in open('itemlist.txt')]
        >>> miner = Mine(identifiers)
        >>> miner.run()
        ...

    A callback can be provided to process the
    :class:`internetarchive.Item <Item>` object as well::

        >>> def print_item_title(item):
        ...     print(item.metadata.get('title'))
        >>> miner = Mine(identifiers, callback=print_item_title)
        >>> miner.run()
        ...

    If no callback is provided, each items metadata is dumped to
    stdout.

    """
    # __init__()
    # ____________________________________________________________________________________
    def __init__(self, identifiers, callback=None, callback_kwargs=None,
                 max_workers=None):
        """
        :type identifiers: list
        :param identifiers: a list of identifiers to concurrently
                            retrieve.

        :type callback: function
        :param callback: The function to call on each
                         :class:`internetarchive.Item <Item>` object as
                         they are retrieved.

        :type callback_kwargs: dict
        :param callback_kwargs: A dict of kwargs for the callback
                                function.

        :type max_workers: int
        :param max_workers: the number of concurrent workers to have
                            fecthing the metadata

        """
        max_workers = 100 if max_workers is None else max_workers
        callback_kwargs = {} if callback_kwargs is None else callback_kwargs

        self.identifiers = identifiers
        self.callback = callback
        self.callback_kwargs = callback_kwargs
        self.http_session = ArchiveSession()
        self.all_identifiers_queued = False

        self.loop = asyncio.get_event_loop()
        self.input_queue = asyncio.Queue(maxsize=1000)
        self.sem = asyncio.Semaphore(max_workers)

    # _queue_identifiers()
    # ____________________________________________________________________________________
    @asyncio.coroutine
    def _queue_identifiers(self):
        """Queue identifiers to be retrieved.

        """
        for identifier in self.identifiers:
            yield From(self.input_queue.put(identifier.strip()))
        self.all_identifiers_queued = True

    # _done_callback()
    # ____________________________________________________________________________________
    def _done_callback(self, future):
        item = future.result()
        if self.callback:
            callback_func = partial(self.callback, item, **self.callback_kwargs)
            callback_future = self.loop.run_in_executor(None, callback_func)
        else:
            json.dump(item._json, sys.stdout)
            sys.stdout.write('\n')

    # _get_future_item()
    # ____________________________________________________________________________________
    @asyncio.coroutine
    def _get_future_item(self):
        """Retrieve future :class:`internetarchive.Item <Item>` objects
        and call `callback` if one is provided.

        """
        asyncio.async(self._queue_identifiers())
        while True:
            if self.all_identifiers_queued and self.input_queue.empty():
                break
            identifier = yield From(self.input_queue.get())

            # Use `functools.partial()` to call `get_item()` with kwargs
            get_item_func = partial(
                get_item, identifier, archive_session=self.http_session
            )
            future = self.loop.run_in_executor(None, get_item_func)
            future.add_done_callback(self._done_callback)

            with (yield From(self.sem)):
                item = yield From(future)
                item.http_session.close()

    # _get_future_item()
    # ____________________________________________________________________________________
    def run(self):
        """Start the event loop, and retrieve all
        :class:`internetarchive.Item <Item>` objects.

        """
        try:
            self.loop.run_until_complete(self._get_future_item())
        except KeyboardInterrupt:
            sys.exit(127)

    # __iter__()
    # ____________________________________________________________________________________
    def __iter__(self):
        raise TypeError("'Mine' object is no longer an iterable. Please refer to the "
                        "internetarchive modules documenation.")
