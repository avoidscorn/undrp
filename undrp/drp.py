"""EPUB+DRP parsing module."""
from collections import namedtuple
from contextlib import contextmanager
from xml.etree import ElementTree as ET
import zipfile

from .io import LazyFile


NS = "{http://www.hp.com/schemas/imaging/ereader/hff}"

Page = namedtuple("Page", ["image", "page_num", "summary", "thumbnail_image", "title"])
PageStub = namedtuple("PageStub", ["image_path", "page_num", "summary", "thumbnail_path", "title"])
ReplicaMap = namedtuple("MetaData", ["pages", "title"])


@contextmanager
def iter_pages(fp):
    with zipfile.ZipFile(fp, "r") as zf, zf.open("OPS/replicaMap.xml", "r") as replica_map_file:
        replica_map = parse_replica_map(replica_map_file)

        def create_page(stub):
            return Page(
                image=stub.image_path and LazyFile(lambda: zf.open(stub.image_path, "r")),
                page_num=stub.page_num,
                summary=stub.summary,
                thumbnail_image=stub.thumbnail_path and LazyFile(lambda: zf.open(stub.thumbnail_path, "r")),
                title=stub.title
            )
        yield (create_page(stub) for stub in replica_map.pages)


def parse_replica_map(f):
    tree = ET.parse(f)

    toc_map = {
        int(x.find(NS + "Page").attrib["pagenum"]): (x.attrib["title"], x.findtext(NS + "Summary"))
        for x in tree.iterfind("./{NS}TOC/{NS}TocEntry".format(NS=NS))
    }
    def create_stub(page_elem):
        path = page_elem.attrib.get("file", None)
        page_num = page_elem.attrib.get("pageNum", None)
        if page_num is not None:
            page_num = int(page_num)
        thumbnail_path = page_elem.attrib.get("thumbFile", None)
        (title, summary) = toc_map.get(page_num, (None, None))
        return PageStub(
            image_path=path and "OPS/" + path,
            page_num=page_num,
            summary=summary,
            thumbnail_path=thumbnail_path and "OPS/" + thumbnail_path,
            title=title
        )
    return ReplicaMap(
        pages=[create_stub(x) for x in tree.iterfind("./{NS}Pages/{NS}Page".format(NS=NS))],
        title=tree.findtext("./{NS}Title".format(NS=NS))
    )
