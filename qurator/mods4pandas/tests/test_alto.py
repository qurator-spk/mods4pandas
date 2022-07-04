from lxml import etree as ET


from qurator.mods4pandas.alto4pandas import alto_to_dict
from qurator.mods4pandas.lib import flatten


def dict_fromstring(x):
   return flatten(alto_to_dict(ET.fromstring(x)))

def test_Page_counts():
    """
    Elements below Layout/Page should be counted
    """
    d = dict_fromstring("""
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
    """)
    assert d['Layout_Page_TextBlock-count'] == 1
    assert d['Layout_Page_TextLine-count'] == 3
    assert d['Layout_Page_String-count'] == 6

def test_Tags_counts():
    d = dict_fromstring("""
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
    """)
    assert d['Tags_NamedEntityTag-count'] == 9

def test_String_TAGREF_counts():
    d = dict_fromstring("""
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
    """)
    assert d['Layout_Page_//alto:String[@TAGREFS]-count'] == 3
    assert d['Layout_Page_String-count'] == 4