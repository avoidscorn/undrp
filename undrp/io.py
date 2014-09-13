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


class TrackingOutputStream(IOBase):
    """File object wrapper that fakes a ``tell`` method.

    This is necessary to get tell() support on stdout.
    """
    def __init__(self, base):
        self._base = base
        self._pos = 0

    def __dir__(self):
        return dir(self._base)

    def __getattr__(self, attr):
        return getattr(self._base, attr)

    def tell(self):
        return self._pos

    def write(self, b):
        num = self._base.write(b)
        self._pos += num or 0
        return num


# Cleanup
del IOBase
