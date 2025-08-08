import re
from pathlib import Path

import pandas as pd
import pytest
from lxml import etree as ET

from ..lib import flatten
from ..mods4pandas import mods_to_dict, process

TESTS_DATA_DIR = Path(__file__).parent / "data"


def dict_fromstring(x):
    """Helper function to parse a MODS XML string to a flattened dict"""
    return flatten(mods_to_dict(ET.fromstring(x)))


def test_single_language_languageTerm():
    d = dict_fromstring(
        """
    <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
        <mods:language>
            <mods:languageTerm authority="iso639-2b" type="code">lat</mods:languageTerm>
            <mods:languageTerm authority="iso639-2b" type="code">ger</mods:languageTerm>
        </mods:language>
    </mods:mods>
    """
    )
    assert d["language_languageTerm"] == {"ger", "lat"}


def test_multitple_language_languageTerm():
    """
    Different languages MAY have multiple mods:language elements.
    See MODS-AP 2.3.1
    """
    d = dict_fromstring(
        """
    <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
        <mods:language><mods:languageTerm authority="iso639-2b" type="code">lat</mods:languageTerm></mods:language>
        <mods:language><mods:languageTerm authority="iso639-2b" type="code">ger</mods:languageTerm></mods:language>
    </mods:mods>
    """
    )
    assert d["language_languageTerm"] == {"ger", "lat"}


def test_role_roleTerm():
    d = dict_fromstring(
        """
    <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
    <mods:name type="personal" valueURI="http://d-nb.info/gnd/117357669">
      <mods:displayForm>Wurm, Mary</mods:displayForm>
      <mods:namePart type="given">Mary</mods:namePart>
      <mods:nameIdentifier type="gbv-ppn">078789583</mods:nameIdentifier>
      <mods:namePart type="family">Wurm</mods:namePart>
      <mods:role>
        <mods:roleTerm authority="marcrelator" type="code">cmp</mods:roleTerm>
      </mods:role>
    </mods:name>
    </mods:mods>
    """
    )
    assert d["name0_role_roleTerm"] == {"cmp"}


def test_multiple_role_roleTerm():
    """
    Multiple mods:role/mods:roleTerm should be merged into one column.
    """
    d = dict_fromstring(
        """
    <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
    <mods:name type="personal" valueURI="http://d-nb.info/gnd/117357669">
      <mods:displayForm>Wurm, Mary</mods:displayForm>
      <mods:namePart type="given">Mary</mods:namePart>
      <mods:nameIdentifier type="gbv-ppn">078789583</mods:nameIdentifier>
      <mods:namePart type="family">Wurm</mods:namePart>
      <mods:role>
        <mods:roleTerm authority="marcrelator" type="code">cmp</mods:roleTerm>
      </mods:role>
      <mods:role>
        <mods:roleTerm authority="marcrelator" type="code">aut</mods:roleTerm>
      </mods:role>
    </mods:name>
    </mods:mods>
    """
    )
    assert d["name0_role_roleTerm"] == {"cmp", "aut"}


def test_scriptTerm():
    """
    Same language using different scripts have one mods:language, with multiple scriptTerms inside.

    See MODS-AP 2.3.1.
    """
    d = dict_fromstring(
        """
    <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
        <mods:language>
            <mods:languageTerm authority="iso639-2b" type="code">ger</mods:languageTerm>
            <mods:scriptTerm authority="iso15924" type="code">215</mods:scriptTerm>
            <mods:scriptTerm authority="iso15924" type="code">217</mods:scriptTerm>
        </mods:language>
        <mods:language>
            <mods:languageTerm authority="iso639-2b" type="code">lat</mods:languageTerm>
            <mods:scriptTerm authority="iso15924" type="code">216</mods:scriptTerm>
        </mods:language>
    </mods:mods>
    """
    )
    assert d["language_scriptTerm"] == {"215", "216", "217"}


def test_recordInfo():
    d = dict_fromstring(
        """
    <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
        <mods:recordInfo>
            <mods:recordIdentifier source="gbv-ppn">PPN610714341</mods:recordIdentifier>
        </mods:recordInfo>
    </mods:mods>
    """
    )
    assert d["recordInfo_recordIdentifier"] == "PPN610714341"


def test_accessCondition():
    d = dict_fromstring(
        """
    <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
        <mods:accessCondition type="use and reproduction">UNKNOWN</mods:accessCondition>
    </mods:mods>
    """
    )
    assert d["accessCondition-use and reproduction"] == "UNKNOWN"


def test_originInfo_no_event_type():
    with pytest.warns(UserWarning) as ws:
        d = dict_fromstring(
            """
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
            <mods:originInfo>
               <mods:place><mods:placeTerm type="text">Berlin</mods:placeTerm></mods:place>
            </mods:originInfo>
        </mods:mods>
        """
        )

    assert d == {}  # empty

    assert len(ws) == 1
    assert isinstance(ws[0].message, Warning)
    assert (
        ws[0].message.args[0]
        == "Filtered {http://www.loc.gov/mods/v3}originInfo element (has no eventType)"
    )


def test_relatedItem():
    d = dict_fromstring(
        """
    <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
        <mods:relatedItem type="original">
            <mods:recordInfo>
                <mods:recordIdentifier source="gbv-ppn">PPN167755803</mods:recordIdentifier>
            </mods:recordInfo>
        </mods:relatedItem>
    </mods:mods>
    """
    )

    assert d["relatedItem-original_recordInfo_recordIdentifier"] == "PPN167755803"

    # mods:relatedItem may also have source="dnb-ppn" recordIdentifiers:
    d = dict_fromstring(
        """
    <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
        <mods:relatedItem type="original">
            <mods:recordInfo>
                <mods:recordIdentifier source="dnb-ppn">1236513355</mods:recordIdentifier>
            </mods:recordInfo>
        </mods:relatedItem>
    </mods:mods>
    """
    )

    assert d["relatedItem-original_recordInfo_recordIdentifier-dnb-ppn"] == "1236513355"


def test_dtypes(tmp_path):
    mets_files = [
        p.absolute().as_posix() for p in (TESTS_DATA_DIR / "mets-mods").glob("*.xml")
    ]
    mods_info_df_parquet = (tmp_path / "test_dtypes_mods_info.parquet").as_posix()
    page_info_df_parquet = (tmp_path / "test_dtypes_page_info.parquet").as_posix()
    process(mets_files, mods_info_df_parquet, page_info_df_parquet)
    mods_info_df = pd.read_parquet(mods_info_df_parquet)
    page_info_df = pd.read_parquet(page_info_df_parquet)

    EXPECTED_TYPES = {
        # mods_info
        r"mets_file": ("object", ["str"]),
        r"titleInfo_title": ("object", ["str"]),
        r"titleInfo_subTitle": ("object", ["str", "NoneType"]),
        r"titleInfo_partName": ("object", ["str", "NoneType"]),
        r"identifier-.*": ("object", ["str", "NoneType"]),
        r"location_.*": ("object", ["str", "NoneType"]),
        r"name\d+_.*roleTerm": ("object", ["ndarray", "NoneType"]),
        r"name\d+_.*": ("object", ["str", "NoneType"]),
        r"relatedItem-.*_recordInfo_recordIdentifier": ("object", ["str", "NoneType"]),
        r"typeOfResource": ("object", ["str", "NoneType"]),
        r"accessCondition-.*": ("object", ["str", "NoneType"]),
        r"originInfo-.*": ("object", ["str", "NoneType"]),
        r".*-count": ("Int64", None),
        r"genre-.*": ("object", ["ndarray", "NoneType"]),
        r"subject-.*": ("object", ["ndarray", "NoneType"]),
        r"language_.*Term": ("object", ["ndarray", "NoneType"]),
        r"classification-.*": ("object", ["ndarray", "NoneType"]),
        # page_info
        r"fileGrp_.*_file_FLocat_href": ("object", ["str", "NoneType"]),
        r"structMap-LOGICAL_TYPE_.*": ("boolean", None),
    }

    def expected_types(c):
        """Return the expected types for column c."""
        for r, types in EXPECTED_TYPES.items():
            if re.fullmatch(r, c):
                edt = types[0]
                einner_types = types[1]
                if einner_types:
                    einner_types = set(einner_types)
                return edt, einner_types
        return None, None

    def check_types(df):
        """Check the types of the DataFrame df."""
        for c in df.columns:
            dt = df.dtypes[c]
            edt, einner_types = expected_types(c)
            print(c, dt, edt)

            assert edt is not None, f"No expected dtype known for column {c} (got {dt})"
            assert dt == edt, f"Unexpected dtype {dt} for column {c} (expected {edt})"

            if edt == "object":
                inner_types = set(type(v).__name__ for v in df[c])
                assert all(
                    it in einner_types for it in inner_types
                ), f"Unexpected inner types {inner_types} for column {c} (expected {einner_types})"

    check_types(mods_info_df)
    check_types(page_info_df)
