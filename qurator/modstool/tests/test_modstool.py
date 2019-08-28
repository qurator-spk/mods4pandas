from qurator.modstool.modstool import mods_to_dict, flatten
import pytest
import xml.etree.ElementTree as ET


def dict_fromstring(x):
   """Helper function to parse a MODS XML string to a flattened dict"""
   return flatten(mods_to_dict(ET.fromstring(x)))

def test_languageTerm():
    """
    Different languages have multiple mods:language elements.
    See MODS-AP 2.3.1
    """
    d = dict_fromstring("""
    <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
        <mods:language><mods:languageTerm authority="iso639-2b" type="code">lat</mods:languageTerm></mods:language>
        <mods:language><mods:languageTerm authority="iso639-2b" type="code">ger</mods:languageTerm></mods:language>
    </mods:mods>
    """)
    assert d['language_languageTerm'] == {'ger', 'lat'}

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
    </mods:mods>
    """)
    assert d['language_scriptTerm'] == {'215', '217'}

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
