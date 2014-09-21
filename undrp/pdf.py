"""PDF output module."""
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


class PdfWriter(object):
    log = logging.getLogger(__name__ + ".PdfWriter")

    def __init__(self, out, *, log=None, owned=True):
        self.out = out
        self.owned = owned
        self._id_stream = count(1)
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
        catalog_id = self._write_catalog(page_tree_root_id=page_tree_root_id)
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

    def write_image_page(self, id=None, *, contents, contents_id=None, image_id=None, media_box=None, parent_id=None):
        self._write_header()

        contents_bytes = contents.read()
        buf = BytesIO(contents_bytes)
        image_obj = Image.open(buf)
        (width, height) = image_obj.size
        color_space = MODE2COLOR_SPACE[image_obj.mode]
        filter = FORMAT2FILTER[image_obj.format]

        content_stream = CONTENT_STREAM_TEMPLATE.format(height=height, width=width)
        if media_box is None:
            media_box = (0, 0, width, height)

        with self.start_object(image_id) as image_id:
            with self.start_dict() as start_dict_entry:
                with start_dict_entry(b"Type"):
                    self._write(b"/XObject")
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
                    self._write(len(contents_bytes))
                with start_dict_entry(b"Filter"):
                    self._write(filter)
            with self.start_stream():
                self._write(contents_bytes)

        with self.start_object(contents_id) as contents_id:
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
            resources="<< /XObject << /Img {} 0 R >> >>".format(image_id)
        )

    def write_page(self, id=None, *, contents_id=None, media_box=None, parent_id=None, resources=None):
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
        return id

    def _write(self, bs):
        if self.out is None:
            raise RuntimeError("PDF writer is closed")

        bs = as_ascii(bs)
        self.out.write(bs)
        self._pos += len(bs)

    def _write_catalog(self, id=None, *, page_tree_root_id):
        with self.start_object(id) as id:
            with self.start_dict() as start_dict_entry:
                with start_dict_entry(b"Type"):
                    self._write(b"/Catalog")
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
        
        def write_entry(id, offset, gen, in_use):
            self._write(as_ascii(id))
            self._write(b" 1\n")
            self._write("{:010d} {:05d} {} \n".format(offset, gen, "n" if in_use else "f").encode("ascii"))

        xref_items = sorted(self._xrefs.items())

        self._write(b"xref\n")
        write_entry(0, 0, 65535, False)
        for (id, offset) in xref_items:
            write_entry(id, offset, 0, True)
        return xref_pos


def as_ascii(obj):
    if isinstance(obj, bytes):
        return obj
    return str(obj).encode("ascii")
