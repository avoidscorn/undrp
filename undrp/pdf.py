"""PDF output module."""
from io import BytesIO
from itertools import count
import logging

from PIL import Image

from .io import TrackingOutputStream


CONTENT_STREAM = b"""\
q
612 0 0 792 0 0 cm
/Img Do
Q
"""

FORMAT2FILTER = {
    "JPEG": b"/DCTDecode"
}

MODE2COLOR_SPACE = {
    "L": b"/DeviceGray",
    "RGB": b"/DeviceRGB"
}

log = logging.getLogger(__name__)


class Result(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def as_ascii(obj):
    return str(obj).encode("ascii")


def write_catalog(out, id_stream, pages_id):
    pos = out.tell()
    catalog_id = next(id_stream)
    write_object_header(out, catalog_id, 0)
    out.write(b"<<\n")
    out.write(b"/Type /Catalog\n")
    out.write(b"/Pages ")
    out.write(as_ascii(pages_id))
    out.write(b" 0 R\n")
    out.write(b">>\n")
    out.write(b"endobj\n")
    return Result(id=catalog_id, xrefs={catalog_id: pos})


def write_footer(out, catalog_id, xrefs):
    log.debug("Writing PDF footer")

    xref_pos = write_xrefs(out, xrefs)

    out.write(b"trailer\n")
    out.write(b"<<\n")
    out.write(b"/Size ")
    out.write(as_ascii(len(xrefs) + 1))
    out.write(b"\n")
    out.write(b"/Root ")
    out.write(as_ascii(catalog_id))
    out.write(b" 0 R\n")
    out.write(b">>\n")
    out.write(b"startxref\n")
    out.write(as_ascii(xref_pos))
    out.write(b"\n")
    out.write(b"%%EOF\n")


def write_header(out):
    log.debug("Writing PDF header")

    out.write(b"%PDF-1.7\n")


def write_image(out, id_stream, image):
    # TODO: Can we avoid reading the whole image into memory? Maybe only parse the first N bytes to get format, width,
    # and height?
    bs = image.read()
    buf = BytesIO(bs)
    image_obj = Image.open(buf)
    (width, height) = image_obj.size

    image_pos = out.tell()
    image_id = next(id_stream)
    write_object_header(out, image_id, 0)
    out.write(b"<<\n")
    out.write(b"/Type /XObject\n")
    out.write(b"/Subtype /Image\n")
    out.write(b"/Width ")
    out.write(as_ascii(width))
    out.write(b"\n")
    out.write(b"/Height ")
    out.write(as_ascii(height))
    out.write(b"\n")
    out.write(b"/ColorSpace ")
    out.write(MODE2COLOR_SPACE[image_obj.mode])
    out.write(b"\n")
    out.write(b"/BitsPerComponent 8\n")
    out.write(b"/Length ")
    out.write(as_ascii(len(bs)))
    out.write(b"\n")
    out.write(b"/Filter ")
    out.write(FORMAT2FILTER[image_obj.format])
    out.write(b"\n")
    out.write(b">>\n")
    out.write(b"stream\n")
    out.write(bs)
    out.write(b"\nendstream\n")
    out.write(b"endobj\n")
    return Result(id=image_id, xrefs={image_id: image_pos})


def write_object_header(out, id, gen):
    out.write(as_ascii(id))
    out.write(b" ")
    out.write(as_ascii(gen))
    out.write(b" obj\n")


def write_objects(out, id_stream, pages):
    xrefs = {}
    pages_id = next(id_stream)
    page_ids = []
    for (i, page) in enumerate(pages, 1):
        log.debug("Processing page %d", i)

        result = write_page(out, id_stream, page, parent_id=pages_id)
        xrefs.update(result.xrefs)
        page_ids.append(result.id)
    xrefs.update(write_pages_object(out, pages_id, page_ids))

    result = write_catalog(out, id_stream, pages_id)
    catalog_id = result.id
    xrefs.update(result.xrefs)

    return Result(catalog_id=catalog_id, xrefs=xrefs)


def write_page(out, id_stream, page, parent_id):
    def write_content_stream():
        content_id = next(id_stream)
        xrefs = {content_id: out.tell()}
        write_object_header(out, content_id, 0)
        out.write(b"<<\n")
        out.write(b"/Length ")
        out.write(as_ascii(len(CONTENT_STREAM)))
        out.write(b"\n")
        out.write(b">>\n")
        out.write(b"stream\n")
        out.write(CONTENT_STREAM)
        out.write(b"endstream\n")
        out.write(b"endobj\n")
        return Result(id=content_id, xrefs=xrefs)

    xrefs = {}

    result = write_image(out, id_stream, page.image)
    image_id = result.id
    xrefs.update(result.xrefs)

    result = write_content_stream()
    content_id = result.id
    xrefs.update(result.xrefs)

    page_id = next(id_stream)
    xrefs[page_id] = out.tell()
    write_object_header(out, page_id, 0)
    out.write(b"<<\n")
    out.write(b"/Type /Page\n")
    out.write(b"/Parent ")
    out.write(as_ascii(parent_id))
    out.write(b" 0 R\n")
    out.write(b"/Resources << /XObject << /Img ")
    out.write(as_ascii(image_id))
    out.write(b" 0 R >> >>\n")
    out.write(b"/Contents ")
    out.write(as_ascii(content_id))
    out.write(b" 0 R\n")
    out.write(b">>\n")
    out.write(b"endobj\n")
    return Result(id=page_id, xrefs=xrefs)


def write_pages_object(out, pages_id, page_ids):
    pos = out.tell()
    write_object_header(out, pages_id, 0)
    out.write(b"<<\n")
    out.write(b"/Type Pages\n")
    out.write(b"/Kids [")
    for (i, page_id) in enumerate(page_ids):
        if i > 0:
            out.write(b" ")
        out.write(as_ascii(page_id))
        out.write(b" 0 R")
    out.write(b"]\n")
    out.write(b"/Count ")
    out.write(as_ascii(len(page_ids)))
    out.write(b"\n")
    out.write(b"/MediaBox [0 0 612 792]\n")
    out.write(b">>\n")
    out.write(b"endobj\n")
    return {pages_id: pos}


def write_pdf(out, pages):
    log.debug("Starting PDF conversion")

    out = TrackingOutputStream(out)
    id_stream = count(1)
    write_header(out)
    result = write_objects(out, id_stream, pages)
    write_footer(out, catalog_id=result.catalog_id, xrefs=result.xrefs)

    log.debug("PDF conversion complete!")

def write_xrefs(out, xrefs):
    def write_xref_entry(id, offset, gen, in_use):
        out.write(as_ascii(id))
        out.write(b" 1\n")
        out.write("{:010d} {:05d} {} \n".format(offset, gen, "n" if in_use else "f").encode("ascii"))

    xref_items = sorted(xrefs.items())

    pos = out.tell()
    out.write(b"xref\n")
    write_xref_entry(0, 0, 65535, False)
    for (id, offset) in xref_items:
        write_xref_entry(id, offset, 0, True)
    return pos
