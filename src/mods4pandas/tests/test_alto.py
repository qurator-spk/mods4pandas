from pathlib import Path
import re
from lxml import etree as ET
import pandas as pd


from ..alto4pandas import alto_to_dict, process
from ..lib import flatten

TESTS_DATA_DIR = Path(__file__).parent / "data"


def dict_fromstring(x):
    return flatten(alto_to_dict(ET.fromstring(x)))


def test_Page_counts():
    """
    Elements below Layout/Page should be counted
    """
    d = dict_fromstring(
        """
    <alto xmlns="http://www.loc.gov/standards/alto/ns-v2#">
      <Layout>
        <Page ID="Page1" PHYSICAL_IMG_NR="1">
            <TextBlock ID="Page1_Block1">
              <TextLine>
                <String STYLE="bold" WC="0.8937500119" CONTENT="Staatsbibliothek" />
              </TextLine>
              <TextLine>
                <String STYLE="bold" WC="0.8899999857" CONTENT="zu" />
                <String STYLE="bold" WC="0.9866666794" CONTENT="Berlin" />
              </TextLine>
              <TextLine>
                <String STYLE="bold" WC="1." CONTENT="WM" />
                <String STYLE="bold" WC="0.8927272558" CONTENT="Preußischer" />
                <String STYLE="bold" WC="0.9058333039" CONTENT="Kulturbesitz" />
              </TextLine>
            </TextBlock>
        </Page>
      </Layout>
    </alto>
    """
    )
    assert d["Layout_Page_TextBlock-count"] == 1
    assert d["Layout_Page_TextLine-count"] == 3
    assert d["Layout_Page_String-count"] == 6


def test_Tags_counts():
    d = dict_fromstring(
        """
    <alto xmlns="http://www.loc.gov/standards/alto/ns-v2#">
      <Tags>
        <NamedEntityTag ID="PER0" LABEL="Pentlings"/>
        <NamedEntityTag ID="LOC1" LABEL="Pentling"/>
        <NamedEntityTag ID="LOC2" LABEL="Hamm"/>
        <NamedEntityTag ID="PER4" LABEL="Hofes Pentling"/>
        <NamedEntityTag ID="LOC5" LABEL="Hofs Pentling"/>
        <NamedEntityTag ID="LOC7" LABEL="Hilbeck"/>
        <NamedEntityTag ID="PER8" LABEL="Hoff"/>
        <NamedEntityTag ID="PER9" LABEL="L i b e r"/>
        <NamedEntityTag ID="PER10" LABEL="Jhesu Christi"/>
      </Tags>
    </alto>
    """
    )
    assert d["Tags_NamedEntityTag-count"] == 9


def test_String_TAGREF_counts():
    d = dict_fromstring(
        """
    <alto xmlns="http://www.loc.gov/standards/alto/ns-v2#">
      <Layout>
      <Page>
      <PrintSpace>
      <TextBlock>
        <TextLine>
          <String CONTENT="Pentlings" HEIGHT="33" HPOS="330" TAGREFS="PER0" VPOS="699" WC="0.4511111081" WIDTH="146"/>
        </TextLine>
        <TextLine>
          <String CONTENT="Pentlings" HEIGHT="33" HPOS="330" TAGREFS="PER0" VPOS="699" WC="0.4511111081" WIDTH="146"/>
          <String CONTENT="Pentlings" HEIGHT="33" HPOS="330" TAGREFS="PER0" VPOS="699" WC="0.4511111081" WIDTH="146"/>
          <String CONTENT="No TAGREF!" />
        </TextLine>
      </TextBlock>
      </PrintSpace>
      </Page>
      </Layout>
    </alto>
    """
    )
    assert d["Layout_Page_//alto:String[@TAGREFS]-count"] == 3
    assert d["Layout_Page_String-count"] == 4


def test_dtypes(tmp_path):
    alto_dir = (TESTS_DATA_DIR / "alto").absolute().as_posix()
    alto_info_df_parquet = (tmp_path / "test_dtypes_alto_info.parquet").as_posix()
    process([alto_dir], alto_info_df_parquet)
    alto_info_df = pd.read_parquet(alto_info_df_parquet)

    EXPECTED_TYPES = {
        r"Description_.*": ("object", ["str", "NoneType"]),
        r"Layout_Page_ID": ("object", ["str", "NoneType"]),
        r"Layout_Page_PHYSICAL_(IMG|IMAGE)_NR": ("object", ["str", "NoneType"]),
        r"Layout_Page_PROCESSING": ("object", ["str", "NoneType"]),
        r"Layout_Page_QUALITY": ("object", ["str", "NoneType"]),
        r"Layout_Page_//alto:String/@WC-.*": ("Float64", None),
        r".*-count": ("Int64", None),
        r"alto_xmlns": ("object", ["str", "NoneType"]),
        r"Layout_Page_(WIDTH|HEIGHT)": ("Int64", None),
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

    check_types(alto_info_df)
