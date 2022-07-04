from lxml import etree as ET


from qurator.modstool.mods4pandas import mets_to_dict
from qurator.modstool.lib import flatten


def dict_fromstring(x):
   """Helper function to parse a METS/MODS XML string to a flattened dict"""
   return flatten(mets_to_dict(ET.fromstring(x)))
   # XXX move to test lib

def test_fileGrp():
    """
    Elements of mets:fileGrp should be counted
    """
    d = dict_fromstring("""
    <mets:mets xmlns:mets="http://www.loc.gov/METS/">

    <mets:fileSec>
    <mets:fileGrp USE="PRESENTATION">
      <mets:file ID="FILE_0001_PRESENTATION" MIMETYPE="image/tiff">
        <mets:FLocat xmlns:xlink="http://www.w3.org/1999/xlink" LOCTYPE="URL" xlink:href="file:///goobi/tiff001/sbb/PPN1678618276/00000001.tif"/>
      </mets:file>
      <mets:file ID="FILE_0002_PRESENTATION" MIMETYPE="image/tiff">
        <mets:FLocat xmlns:xlink="http://www.w3.org/1999/xlink" LOCTYPE="URL" xlink:href="file:///goobi/tiff001/sbb/PPN1678618276/00000002.tif"/>
      </mets:file>
      <mets:file ID="FILE_0003_PRESENTATION" MIMETYPE="image/tiff">
        <mets:FLocat xmlns:xlink="http://www.w3.org/1999/xlink" LOCTYPE="URL" xlink:href="file:///goobi/tiff001/sbb/PPN1678618276/00000003.tif"/>
      </mets:file>
    </mets:fileGrp>
    </mets:fileSec>
    </mets:mets>
    """)
    assert d['fileSec_fileGrp-PRESENTATION-count'] == 3
