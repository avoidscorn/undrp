"""PDF output module."""
from abc import ABCMeta, abstractmethod
from codecs import BOM_UTF16_BE
from contextlib import contextmanager
from io import BytesIO
from itertools import count
import logging

from PIL import Image

CONTENT_STREAM_TEMPLATE = """\
q
{width} 0 0 {height} 0 0 cm
/Img Do
Q"""

FORMAT2FILTER = {
    "JPEG": b"/DCTDecode"
}

MODE2COLOR_SPACE = {
    "L": b"/DeviceGray",
    "RGB": b"/DeviceRGB"
}


class PdfDest(object, metaclass=ABCMeta):
    @abstractmethod
    def write(self, write):
        pass


class FitH(PdfDest):
    def __init__(self, *, top=None):
        self.top = top

    def write(self, write):
        write(b"/FitH ")
        if self.top is not None:
            write(self.top)
        else:
            write(b"null")


class PdfOutlineItem(object):
    __slots__ = ["children", "location", "page_id", "title"]

    def __init__(self, *, children=None, location=None, page_id=None, title=None):
        if children is None:
            children = []

        self.children = children
        self.location = location
        self.page_id = page_id
        self.title = title

    @property
    def count(self):
        return sum(child.count for child in self.children)


class PdfWriter(object):
    log = logging.getLogger(__name__ + ".PdfWriter")

    def __init__(self, out, *, log=None, owned=True):
        self.out = out
        self.owned = owned
        self._id_stream = count(1)
        self._outline_root_id = None
        self._page_ids = []
        self._page_tree_root_id = None
        self._pos = 0
        self._xrefs = {}
        if log is not None:
            self.log = log

    def __enter__(self):
        self._write_header()
        return self

    # noinspection PyUnusedLocal
    def __exit__(self, *args):
        self.close()

    def close(self):
        if self.out is None:
            return

        self._write_header()

        self.log.debug("Closing")
        page_tree_root_id = self._write_page_tree()
        catalog_id = self._write_catalog(outline_root_id=self._outline_root_id, page_tree_root_id=page_tree_root_id)
        xref_pos = self._write_xrefs()
        self._write_trailer(catalog_id=catalog_id)
        self._write_startxref(xref_pos=xref_pos)
        self._write_footer()
        if self.owned:
            self.out.close()
        self.out = None

    def next_object_id(self):
        return next(self._id_stream)

    @contextmanager
    def start_array(self):
        self._write_header()

        first = True

        @contextmanager
        def start_array_element():
            nonlocal first
            if first:
                first = False
            else:
                self._write(b" ")
            yield

        self._write(b"[")
        yield start_array_element
        self._write(b"]")

    @contextmanager
    def start_dict(self):
        self._write_header()

        @contextmanager
        def start_dict_entry(key):
            self._write(b"/")
            self._write(key)
            self._write(b" ")
            yield
            self._write(b"\n")

        self._write(b"<<\n")
        yield start_dict_entry
        self._write(b">>\n")

    @contextmanager
    def start_object(self, id=None):
        self._write_header()

        if id is None:
            id = self.next_object_id()
        self._xrefs[id] = self._pos
        self._write(as_ascii(id))
        self._write(b" 0 obj\n")
        yield id
        self._write(b"endobj\n")

    @contextmanager
    def start_stream(self):
        self._write_header()

        self._write(b"stream\n")
        yield
        self._write(b"\nendstream\n")

    def write_image_page(
        self,
        id=None,
        *,
        image,
        media_box=None,
        parent_id=None,
        thumbnail=None
    ):
        self._write_header()

        thumbnail_result = thumbnail and self._write_image(image=thumbnail, subtype=False, type=False)
        image_result = self._write_image(image=image, subtype=True, type=True)

        content_stream = CONTENT_STREAM_TEMPLATE.format(height=image_result.height, width=image_result.width)
        if media_box is None:
            media_box = (0, 0, image_result.width, image_result.height)

        with self.start_object() as contents_id:
            with self.start_dict() as start_dict_entry:
                with start_dict_entry(b"Length"):
                    self._write(as_ascii(len(content_stream)))
            with self.start_stream():
                self._write(content_stream)

        return self.write_page(
            id,
            contents_id=contents_id,
            media_box=media_box,
            parent_id=parent_id,
            resources="<< /XObject << /Img {} 0 R >> >>".format(image_result.id),
            thumbnail_id=thumbnail_result and thumbnail_result.id
        )

    def write_outline(self, items):
        if self._outline_root_id is not None:
            raise RuntimeError("Outline already written")

        items = list(items)
        if not items:
            return

        root_id = self.next_object_id()

        # Use depth-first traversal to get ids for each item.
        item2id = {}
        def visit_item(item):
            if item in item2id:
                raise RuntimeError("Duplicate outline item detected")
            item2id[item] = self.next_object_id()
            for child in item.children:
                visit_item(child)
        for item in items:
            visit_item(item)

        def write_item(item, *, next_id, parent_id, prev_id):
            item_id = item2id[item]

            with self.start_object(item_id):
                with self.start_dict() as start_dict_entry:
                    with start_dict_entry(b"Title"):
                        self._write_str(item.title)
                    with start_dict_entry(b"Parent"):
                        self._write_ref(parent_id)
                    if prev_id is not None:
                        with start_dict_entry(b"Prev"):
                            self._write_ref(prev_id)
                    if next_id is not None:
                        with start_dict_entry(b"Next"):
                            self._write_ref(next_id)
                    if item.children:
                        with start_dict_entry(b"First"):
                            self._write_ref(item2id[item.children[0]])
                        with start_dict_entry(b"Last"):
                            self._write_ref(item2id[item.children[-1]])
                        with start_dict_entry(b"Count"):
                            self._write(item.count)
                    with start_dict_entry(b"Dest"):
                        with self.start_array() as start_array_item:
                            with start_array_item():
                                self._write_ref(item.page_id)
                            with start_array_item():
                                item.location.write(self._write)

            write_items(item.children, parent_id=item_id)

        def write_items(items, parent_id):
            for i in range(len(items)):
                write_item(
                    items[i],
                    next_id=item2id[items[i + 1]] if i + 1 < len(items) else None,
                    parent_id=parent_id,
                    prev_id=item2id[items[i - 1]] if i > 0 else None
                )

        with self.start_object(root_id):
            with self.start_dict() as start_dict_entry:
                with start_dict_entry(b"Type"):
                    self._write(b"/Outlines")
                with start_dict_entry(b"First"):
                    self._write_ref(item2id[items[0]])
                with start_dict_entry(b"Last"):
                    self._write_ref(item2id[items[-1]])
                with start_dict_entry(b"Count"):
                    self._write(len(item2id))
        write_items(items, parent_id=root_id)
        self._outline_root_id = root_id
        return root_id

    def write_page(
        self,
        id=None,
        *,
        contents_id=None,
        media_box=None,
        parent_id=None,
        resources=None,
        thumbnail_id=None
    ):
        self._write_header()

        if parent_id is None:
            parent_id = self._page_tree_root_id = self._page_tree_root_id or self.next_object_id()

        with self.start_object(id) as id:
            self._page_ids.append(id)

            with self.start_dict() as start_dict_entry:
                with start_dict_entry(b"Type"):
                    self._write(b"/Page")
                with start_dict_entry(b"Parent"):
                    self._write_ref(parent_id)
                if resources is not None:
                    with start_dict_entry(b"Resources"):
                        self._write(resources)
                if media_box is not None:
                    (ll_x, ll_y, ur_x, ur_y) = media_box
                    with start_dict_entry(b"MediaBox"):
                        with self.start_array() as start_array_element:
                            with start_array_element():
                                self._write(ll_x)
                            with start_array_element():
                                self._write(ll_y)
                            with start_array_element():
                                self._write(ur_x)
                            with start_array_element():
                                self._write(ur_y)
                if contents_id is not None:
                    with start_dict_entry(b"Contents"):
                        if hasattr(contents_id, "__iter__"):
                            with self.start_array() as start_array_element:
                                for id in contents_id:
                                    with start_array_element():
                                        self._write_ref(id)
                        else:
                            self._write_ref(contents_id)
                if thumbnail_id is not None:
                    with start_dict_entry(b"Thumb"):
                        self._write_ref(thumbnail_id)
        return id

    def _write(self, bs):
        if self.out is None:
            raise RuntimeError("PDF writer is closed")

        bs = as_ascii(bs)
        self.out.write(bs)
        self._pos += len(bs)

    def _write_catalog(self, id=None, *, outline_root_id=None, page_tree_root_id):
        with self.start_object(id) as id:
            with self.start_dict() as start_dict_entry:
                with start_dict_entry(b"Type"):
                    self._write(b"/Catalog")
                if outline_root_id is not None:
                    with start_dict_entry(b"Outlines"):
                        self._write_ref(outline_root_id)
                with start_dict_entry(b"Pages"):
                    self._write_ref(page_tree_root_id)
        return id

    def _write_footer(self):
        self.log.debug("Writing PDF footer")
        self._write(b"%%EOF")

    def _write_header(self):
        if self._pos != 0:
            return

        self.log.debug("Writing PDF header")
        self._write(b"%PDF-1.7\n")

    def _write_image(self, id=None, *, image, subtype=True, type=True):
        image_bytes = image.read()
        buf = BytesIO(image_bytes)
        image_obj = Image.open(buf)
        (width, height) = image_obj.size
        color_space = MODE2COLOR_SPACE[image_obj.mode]
        filter = FORMAT2FILTER[image_obj.format]

        with self.start_object(id) as id:
            with self.start_dict() as start_dict_entry:
                if type:
                    with start_dict_entry(b"Type"):
                        self._write(b"/XObject")
                if subtype:
                    with start_dict_entry(b"Subtype"):
                        self._write(b"/Image")
                with start_dict_entry(b"Width"):
                    self._write(width)
                with start_dict_entry(b"Height"):
                    self._write(height)
                with start_dict_entry(b"ColorSpace"):
                    self._write(color_space)
                with start_dict_entry(b"BitsPerComponent"):
                    self._write(b"8")
                with start_dict_entry(b"Length"):
                    self._write(len(image_bytes))
                with start_dict_entry(b"Filter"):
                    self._write(filter)
            with self.start_stream():
                self._write(image_bytes)

        return Result(height=height, id=id, width=width)

    def _write_page_tree(self):
        self.log.debug("Writing PDF page tree")
        with self.start_object(self._page_tree_root_id) as id:
            self._page_tree_root_id = id

            with self.start_dict() as start_dict_entry:
                with start_dict_entry(b"Type"):
                    self._write(b"/Pages")
                with start_dict_entry(b"Kids"):
                    with self.start_array() as start_array_element:
                        for page_id in self._page_ids:
                            with start_array_element():
                                self._write_ref(page_id)
                with start_dict_entry(b"Count"):
                    self._write(len(self._page_ids))
        return id

    def _write_ref(self, id):
        self._write(as_ascii(id))
        self._write(b" 0 R")

    def _write_startxref(self, xref_pos):
        self.log.debug("Writing PDF 'startxref' section")
        self._write(b"startxref\n")
        self._write(as_ascii(xref_pos))
        self._write(b"\n")

    def _write_str(self, chars):
        bs = as_ascii(chars)
        # bs = chars.encode("utf-16be")
        bs = bs.replace(b"\\", rb"\\")
        bs = bs.replace(b"(", rb"\(")
        bs = bs.replace(b")", rb"\)")

        self._write(b"(")
        # self._write(BOM_UTF16_BE)
        self._write(bs)
        self._write(b")")

    def _write_trailer(self, catalog_id):
        self.log.debug("Writing PDF 'trailer' section")
        self._write(b"trailer\n")
        self._write(b"<<\n")
        self._write(b"/Size ")
        self._write(len(self._xrefs) + 1)
        self._write(b"\n")
        self._write(b"/Root ")
        self._write(catalog_id)
        self._write(b" 0 R\n")
        self._write(b">>\n")

    def _write_xrefs(self):
        xref_pos = self._pos

        def group_sequential(xrefs):
            xrefs = sorted(xrefs)

            pending = []
            for (id, offset) in xrefs:
                if pending and id != pending[-1][0] + 1:
                    yield pending
                    pending = []
                pending.append((id, offset))
            if pending:
                yield pending

        def write_entry(offset, gen, in_use):
            self._write("{:010d} {:05d} {} \n".format(offset, gen, "n" if in_use else "f"))

        def write_group_header(start_id, count):
            self._write(start_id)
            self._write(b" ")
            self._write(count)
            self._write(b"\n")

        self._write(b"xref\n")
        write_group_header(0, 1)
        write_entry(0, 65535, False)
        for group in group_sequential(self._xrefs.items()):
            write_group_header(group[0][0], len(group))
            for (_, offset) in group:
                write_entry(offset, 0, True)
        return xref_pos


class Result(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def as_ascii(obj):
    if isinstance(obj, bytes):
        return obj
    return str(obj).encode("ascii")


# Cleanup
del ABCMeta
del abstractmethod
