import pytest
import xml.etree.ElementTree as ET


from .. import mods_to_dict, flatten


def dict_fromstring(x):
    """Helper function to parse a MODS XML string to a flattened dict"""
    return flatten(mods_to_dict(ET.fromstring(x)))

def test_single_language_languageTerm():
    d = dict_fromstring("""
    <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
        <mods:language>
            <mods:languageTerm authority="iso639-2b" type="code">lat</mods:languageTerm>
            <mods:languageTerm authority="iso639-2b" type="code">ger</mods:languageTerm>
        </mods:language>
    </mods:mods>
    """)
    assert d['language_languageTerm'] == {'ger', 'lat'}

def test_multitple_language_languageTerm():
    """
    Different languages MAY have multiple mods:language elements.
    See MODS-AP 2.3.1
    """
    d = dict_fromstring("""
    <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
        <mods:language><mods:languageTerm authority="iso639-2b" type="code">lat</mods:languageTerm></mods:language>
        <mods:language><mods:languageTerm authority="iso639-2b" type="code">ger</mods:languageTerm></mods:language>
    </mods:mods>
    """)
    assert d['language_languageTerm'] == {'ger', 'lat'}

def test_role_roleTerm():
    d = dict_fromstring("""
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
    """)
    assert d['name0_role_roleTerm'] == {'cmp'}

def test_multiple_role_roleTerm():
    """
    Multiple mods:role/mods:roleTerm should be merged into one column.
    """
    d = dict_fromstring("""
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
    """)
    assert d['name0_role_roleTerm'] == {'cmp', 'aut'}

def test_scriptTerm():
    """
    Same language using different scripts have one mods:language, with multiple scriptTerms inside.

    See MODS-AP 2.3.1.
    """
    d = dict_fromstring("""
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
    """)
    assert d['language_scriptTerm'] == {'215', '216', '217'}

def test_recordInfo():
    d = dict_fromstring("""
    <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
        <mods:recordInfo>
            <mods:recordIdentifier source="gbv-ppn">PPN610714341</mods:recordIdentifier>
        </mods:recordInfo>
    </mods:mods>
    """)
    assert d['recordInfo_recordIdentifier'] == 'PPN610714341'

def test_accessCondition():
    d = dict_fromstring("""
    <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
        <mods:accessCondition type="use and reproduction">UNKNOWN</mods:accessCondition>
    </mods:mods>
    """)
    assert d['accessCondition-use and reproduction'] == 'UNKNOWN'

def test_originInfo_no_event_type():
    with pytest.warns(UserWarning) as ws:
        d = dict_fromstring("""
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
            <mods:originInfo>
               <mods:place><mods:placeTerm type="text">Berlin</mods:placeTerm></mods:place>
            </mods:originInfo>
        </mods:mods>
        """)

    assert d == {}  # empty

    assert len(ws) == 1
    assert ws[0].message.args[0] == 'Filtered {http://www.loc.gov/mods/v3}originInfo element (has no eventType)'
