
ABOUT
=====

``undrp`` converts Barnes & Noble's proprietary fixed-layout EPUB files (aka
`BNFixedFormat <https://code.google.com/p/epub-revision/wiki/BNFixedFormat>`_ or DRP) to more generic formats such as
PDF. The goal is to allow you to read these e-books in non-Barnes & Noble e-readers.

INSTALLATION
============

``undrp`` requires Python 3.3 or later. You can install it via pip::

    pip install undrp


USAGE
=====

You can run ``undrp`` like the UNIX ``cp`` command.

Convert a single file (explicit target filename)::

    undrp /path/to/somebook.epub somebook.pdf

Convert a single file (implicit target filename)::

    undrp /path/to/somebook.epub .

Convert multiple files::

    undrp /path/to/*.epub .

Convert a single file, outputting to stdout::

    undrp /path/to/somebook.epub -


LIMITATIONS
===========

``undrp`` is still in its infancy, so there are quite a few limitations:

* Can only handle EPUB files where all the pages are JPEG images.
* Error reporting is terrible.
* Cannot convert EPUB files with DRM. You will need to strip the DRM off first.
* Can only output to PDF format.
