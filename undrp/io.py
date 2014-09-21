from io import IOBase


class LazyFile(object):
    """File object wrapper that lazily opens its underlying file object when needed.

    This necessary if you want lots of file-like objects, only a few of those file-like objects will be used at a time,
    and are worried about resource exhaustion caused by opening all the file objects at once.
    """
    def __init__(self, factory):
        super(LazyFile, self).__init__()
        self._base_obj = None
        self._factory = factory

    def __dir__(self):
        return dir(self._base)

    def __getattr__(self, attr):
        return getattr(self._base, attr)

    @property
    def _base(self):
        if self._base_obj is None:
            self._base_obj = self._factory()
            self._factory = None
        return self._base_obj


# Cleanup
del IOBase
