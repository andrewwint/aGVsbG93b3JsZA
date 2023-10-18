from collections import OrderedDict


def _do_nothing(key, value):
    pass


class LRUCache(OrderedDict):
    """
    A size limited dictionary that allows evicted items to be processed with a callback function.
    """

    def __init__(self, max_items, eviction_callback=_do_nothing, *args, **kwargs):
        """
        Parameters:
            max_items: The maximum number of items allowed in the dictionary.
            eviction_callback: A callback function that takes a (key, value) pair and processes
                               them when a record is evicted from the cache.
        """
        super().__init__(*args, **kwargs)
        self._max_items = max_items
        self._item_count = len(self)
        self._eviction_callback = eviction_callback

    def __setitem__(self, key, value):
        # If the key is not in the cache make sure there is room
        if key not in self:
            # Free up some space if cache is full
            if self._item_count >= self._max_items:
                # Remove the front item of the cache since it is the
                # least recently used.
                self.popitem(last=False)

        # Set the item like normal
        super().__setitem__(key, value)

        self._item_count += 1

        # Ensure the current key is at the end of the cache
        # Keys at the end are the most recently used
        self.move_to_end(key)

    def __delitem__(self, key):
        # Run the callback function of the deleted item
        self._eviction_callback(key, self[key])

        # Delete the item like normal
        super().__delitem__(key)

        # Track the decrease in current cache size
        self._item_count -= 1

    def popitem(self, last=True):
        # pop the item like normal
        key, value = super().popitem(last)
        # Run the callback function of the popped item
        self._eviction_callback(key, value)
        # Track the decrease in current cache size
        self._item_count -= 1
        return key, value

    def flush(self):
        """
        Evicts all currently cached items.
        """
        while self:
            self.popitem(False)


def batched(iterable, n):
    """
    A backport of python 3.12s itertools.batched
    https://docs.python.org/3.12/library/itertools.html#itertools.batched
    """
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch
