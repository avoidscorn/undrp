"""
Usage:
undrp [options] [--] SOURCE... TARGET
undrp [options] -t TARGET [--] SOURCE...

Convert DRP-formatted EPUB files to PDF.

Arguments:
  SOURCE  path to source EPUB file (the file must be DRP-formatted)
  DEST    destination file or directory. If a directory, the destination file(s) will have the same filename as SOURCE
          with a different extension.

Options:
  -h, --help           Show this help message
  -V, --version        Show version and exit
  --start N            Start at page N [default: 1]
  --stop N             Stop at page N
  -t, --target TARGET  Output to TARGET
  -v, --verbose        Show status messages
"""
import argparse
from contextlib import contextmanager, ExitStack
from itertools import islice
import logging
import os.path
import sys

from undrp import __version__
from undrp.drp import iter_pages
from undrp.pdf import FitH, PdfOutlineItem, PdfWriter


log = logging.getLogger(__name__)


def create_outline_entry(*, page, page_id):
    if page.summary and page.title:
        title = "{} - {}".format(page.title, page.summary)
    elif page.summary:
        title = page.summary
    elif page.title:
        title = page.title
    else:
        return None

    return PdfOutlineItem(location=FitH(), page_id=page_id, title=title)


def main(argv=None):
    try:
        if argv is None:
            argv = sys.argv

        args = parse_args(argv[1:])

        if args.verbosity >= 2:
            level = logging.DEBUG
        elif args.verbosity >= 1:
            level = logging.INFO
        else:
            level = logging.WARNING
        logging.basicConfig(format="%(message)s", level=level, stream=sys.stderr)

        if args.version:
            print("{} {}".format(__package__, __version__))
            return 0

        for source in args.sources:
            target_path = get_target_path(args.target, source=source)
            log.info("%r -> %r", source, target_path)

            with ExitStack() as stack:
                output_file = stack.enter_context(open_target(target_path))
                pages = stack.enter_context(iter_pages(source))
                pdf_writer = stack.enter_context(PdfWriter(output_file))

                outline_items = []
                for (i, page) in enumerate(islice(pages, args.start - 1, args.stop), 1):
                    log.debug("Processing page %d", i)
                    page_id = pdf_writer.write_image_page(image=page.image, thumbnail=page.thumbnail_image)

                    outline_item = create_outline_entry(page=page, page_id=page_id)
                    if outline_item is not None:
                        outline_items.append(outline_item)
                if outline_items:
                    pdf_writer.write_outline(outline_items)

        return 0
    except ArgsError as e:
        print(e, file=sys.stderr)
        return 2
    except (IOError, OSError) as e:
        print(e, file=sys.stderr)
        return 1


def get_target_path(path, source):
    if path == "-":
        return path

    if os.path.isdir(path):
        basename = os.path.splitext(os.path.basename(source))[0]
        return os.path.join(path, basename) + ".pdf"

    return path


@contextmanager
def open_target(path):
    if path == "-":
        yield sys.stdout.buffer
        return

    with open(path, "wb") as f:
        yield f


#
# Argument parsing
#

# Similar to the ``cp`` command, we support two forms of usage, with --target (in which case all positional arguments
# are source files) or without --target (in which case the last positional argument the destination file/directory).
# argparse does not support this natively, so create a very lax argument parser and validate afterward. ``docopt`` does
# support multiple usage modes, but https://github.com/docopt/docopt/issues/172 prevents it from working in
# our specific case, and it is harder to work around in ``docopt``.
argparser = argparse.ArgumentParser(
    description="Convert DRP-formatted EPUB files to PDF.",
    usage="\n%(prog)s [options] SOURCE [SOURCE ...] TARGET\n%(prog)s [options] -t TARGET SOURCE [SOURCE ...]"
)
argparser.add_argument(
    "--start",
    default=1,
    help="Start at page %(metavar)s [default: %(default)s]",
    metavar="N",
    type=int
)
argparser.add_argument(
    "--stop",
    default=None,
    help="Stop at page %(metavar)s [default: stop at last page]",
    metavar="N",
    type=int
)
argparser.add_argument("-t", "--target", default=None, help="Destination file or folder")
argparser.add_argument("-v", "--verbose", action="count", default=0, dest="verbosity", help="Increase verbosity")
argparser.add_argument("-V", "--version", action="store_true", default=False, help="Display version and exit")
argparser.add_argument(
    "sources",
    help="path to source EPUB file (the file must be DRP-formatted)",
    metavar="SOURCE",
    nargs="*"
)
# This argument should never get used while parsing (due to the above nargs="*"), but it lets us supply help text.
argparser.add_argument(
    "target_not_used",
    help=(
        "destination file or directory. If a directory, the destination file(s) will have the same filename as SOURCE "
        "with a different extension."
    ),
    metavar="TARGET",
    nargs="?"
)


class ArgsError(Exception):
    pass


def parse_args(args):
    retval = argparser.parse_args(args)

    if retval.version:
        return retval

    if not retval.sources:
        argparser.error("Missing source file(s)")

    if retval.target is None and retval.sources:
        retval.target = retval.sources.pop()
        if not retval.sources:
            argparser.error("Missing destination file/directory")

    for source in retval.sources:
        if not os.path.exists(source):
            raise ArgsError("Source file does not exist: {}".format(source))
        if not os.path.isfile(source):
            raise ArgsError("Source path is not a file: {}".format(source))

    if len(retval.sources) > 1 and (retval.target == "-" or not os.path.isdir(retval.target)):
        raise ArgsError("Destination must be a directory: {}".format(retval.target))

    dirname = os.path.dirname(retval.target)
    if dirname and not os.path.isdir(dirname):
        raise ArgsError("Destination's parent directory must exist: {}".format(retval.target))

    return retval


if __name__ == "__main__":
    sys.exit(main())
