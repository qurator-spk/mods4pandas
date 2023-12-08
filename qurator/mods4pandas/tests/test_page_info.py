from pathlib import Path

from lxml import etree as ET

from qurator.mods4pandas.mods4pandas import pages_to_dict


TESTS_DATA_DIR = Path(__file__).parent / "data"


def test_page_info():
    """Test creation of page_info"""
    mets = ET.parse(TESTS_DATA_DIR / "mets-mods" / "PPN821507109-1361-pages.xml")
    page_info = pages_to_dict(mets)

    # We have 1361 pages for this one work.
    assert len(page_info) == 1361
    assert all(p["ppn"] == "PPN821507109" for p in page_info)

    # Look closer at an interesting page
    from pprint import pprint; pprint(page_info[0])
    page_info_page = next(p for p in page_info if p["ID"] == "PHYS_0005")

    assert page_info_page["fileGrp_PRESENTATION_file_FLocat_href"] == "file:///goobi/tiff001/sbb/PPN821507109/00000005.tif"

    # This is a title page with an illustration, check that we correctly got this info from the
    # structMap.
    struct_types = sorted(k.removeprefix("structMap-LOGICAL_TYPE_") for k, v in page_info_page.items() if k.startswith("structMap-LOGICAL_TYPE_") and v == 1)
    assert struct_types == ["illustration", "monograph", "title_page"]
