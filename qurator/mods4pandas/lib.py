from itertools import groupby
import re
import warnings
from typing import List, Sequence, MutableMapping, Dict

import pandas as pd
import numpy as np
from lxml import etree as ET


__all__ = ["ns"]


ns = {
    'mets': 'http://www.loc.gov/METS/',
    'mods': 'http://www.loc.gov/mods/v3',
    "alto": "http://www.loc.gov/standards/alto/ns-v2",
    "xlink": "http://www.w3.org/1999/xlink",
}



class TagGroup:
    """Helper class to simplify the parsing and checking of MODS metadata"""

    def __init__(self, tag, group: List[ET.Element]):
        self.tag = tag
        self.group = group

    def to_xml(self):
        return '\n'.join(str(ET.tostring(e), 'utf-8').strip() for e in self.group)

    def __str__(self):
        return f"TagGroup with content:\n{self.to_xml()}"

    def is_singleton(self):
        if len(self.group) != 1:
            raise ValueError('More than one instance: {}'.format(self))
        return self

    def has_no_attributes(self):
        return self.has_attributes({})

    def has_attributes(self, attrib):
        if not isinstance(attrib, Sequence):
            attrib = [attrib]
        if not all(e.attrib in attrib for e in self.group):
            raise ValueError('One or more element has unexpected attributes: {}'.format(self))
        return self

    def ignore_attributes(self):
        # This serves as documentation for now.
        return self

    def sort(self, key=None, reverse=False):
        self.group = sorted(self.group, key=key, reverse=reverse)
        return self

    def text(self, separator='\n'):
        t = ''
        for e in self.group:
            if t != '':
                t += separator
            if e.text:
                t += e.text
        return t

    def text_set(self):
        return {e.text for e in self.group}

    def descend(self, raise_errors):
        return _to_dict(self.is_singleton().group[0], raise_errors)

    def filter(self, cond, warn=None):
        new_group = []
        for e in self.group:
            if cond(e):
                new_group.append(e)
            else:
                if warn:
                    warnings.warn('Filtered {} element ({})'.format(self.tag, warn))
        return TagGroup(self.tag, new_group)

    def force_singleton(self, warn=True):
        if len(self.group) == 1:
            return self
        else:
            if warn:
                warnings.warn('Forced single instance of {}'.format(self.tag))
            return TagGroup(self.tag, self.group[:1])

    RE_ISO8601_DATE = r'^\d{2}(\d{2}|XX)(-\d{2}-\d{2})?$'  # Note: Includes non-specific century dates like '18XX'
    RE_GERMAN_DATE = r'^(?P<dd>\d{2})\.(?P<mm>\d{2})\.(?P<yyyy>\d{4})$'

    def fix_date(self):

        for e in self.group:
            if e.attrib.get('encoding') == 'w3cdtf':
                # This should be 'iso8601' according to MODS-AP 2.3.1
                warnings.warn('Changed w3cdtf encoding to iso8601')
                e.attrib['encoding'] = 'iso8601'

        new_group = []
        for e in self.group:
            if e.attrib.get('encoding') == 'iso8601' and re.match(self.RE_ISO8601_DATE, e.text):
                new_group.append(e)
            elif re.match(self.RE_ISO8601_DATE, e.text):
                warnings.warn('Added iso8601 encoding to date {}'.format(e.text))
                e.attrib['encoding'] = 'iso8601'
                new_group.append(e)
            elif re.match(self.RE_GERMAN_DATE, e.text):
                warnings.warn('Converted date {} to iso8601 encoding'.format(e.text))
                m = re.match(self.RE_GERMAN_DATE, e.text)
                e.text = '{}-{}-{}'.format(m.group('yyyy'), m.group('mm'), m.group('dd'))
                e.attrib['encoding'] = 'iso8601'
                new_group.append(e)
            else:
                warnings.warn('Not a iso8601 date: "{}"'.format(e.text))
                new_group.append(e)
        self.group = new_group

        # Notes:
        # - There are dates with the misspelled qualifier 'aproximate'
        # - Rough periods are sometimes given either by:
        #   - years like '19xx'
        #   - or 'approximate' date ranges with point="start"/"end" attributes set
        #     (this could be correct according to MODS-AP 2.3.1)
        # - Some very specific dates like '06.08.1820' are sometimes given the 'approximate' qualifier
        # - Sometimes, approximate date ranges are given in the text "1785-1800 (ca.)"

        return self

    def fix_event_type(self):
        # According to MODS-AP 2.3.1, every originInfo should have its eventType set.
        # Fix this for special cases.

        for e in self.group:
            if e.attrib.get('eventType') is None:
                try:
                    if e.find('mods:publisher', ns).text.startswith('Staatsbibliothek zu Berlin') and \
                            e.find('mods:edition', ns).text == '[Electronic ed.]':
                        e.attrib['eventType'] = 'digitization'
                        warnings.warn('Fixed eventType for electronic ed.')
                        continue
                except AttributeError:
                    pass
                try:
                    if e.find('mods:dateIssued', ns) is not None:
                        e.attrib['eventType'] = 'publication'
                        warnings.warn('Fixed eventType for an issued origin')
                        continue
                except AttributeError:
                    pass
                try:
                    if e.find('mods:dateCreated', ns) is not None:
                        e.attrib['eventType'] = 'production'
                        warnings.warn('Fixed eventType for a created origin')
                        continue
                except AttributeError:
                    pass
        return self

    def fix_script_term(self):
        for e in self.group:
            # MODS-AP 2.3.1 is not clear about this, but it looks like that this should be lower case.
            if e.attrib['authority'] == 'ISO15924':
                e.attrib['authority'] = 'iso15924'
                warnings.warn('Changed scriptTerm authority to lower case')
        return self

    def merge_sub_tags_to_set(self):
        from .mods4pandas import mods_to_dict
        value = {}

        sub_dicts = [mods_to_dict(e) for e in self.group]
        sub_tags = {k for d in sub_dicts for k in d.keys()}
        for sub_tag in sub_tags:
            s = set()
            for d in sub_dicts:
                v = d.get(sub_tag)
                if v:
                    # There could be multiple scriptTerms in one language element, e.g. Antiqua and Fraktur in a
                    # German language document.
                    if isinstance(v, set):
                        s.update(v)
                    else:
                        s.add(v)
            value[sub_tag] = s
        return value

    def attributes(self):
        """
        Return a merged dict of all attributes of the tag group.

        Probably most useful if used on a singleton, for example:

            value['Page'] = TagGroup(tag, group).is_singleton().attributes()
        """
        attrib = {}
        for e in self.group:
            for a, v in e.attrib.items():
                a_localname = ET.QName(a).localname
                attrib[a_localname] = v
        return attrib

    def subelement_counts(self):
        counts = {}
        for e in self.group:
            for x in e.iter():
                tag = ET.QName(x.tag).localname
                key = f"{tag}-count"
                counts[key] = counts.get(key, 0) + 1
        return counts

    def xpath_statistics(self, xpath_expr, namespaces):
        """
        Extract values and calculate statistics

        Extract values using the given XPath expression, convert them to float and return descriptive
        statistics on the values.
        """
        values = []
        for e in self.group:
            r = e.xpath(xpath_expr, namespaces=namespaces)
            values += r
        values = np.array([float(v) for v in values])

        statistics = {}
        if values.size > 0:
            statistics[f'{xpath_expr}-mean'] = np.mean(values)
            statistics[f'{xpath_expr}-median'] = np.median(values)
            statistics[f'{xpath_expr}-std'] = np.std(values)
            statistics[f'{xpath_expr}-min'] = np.min(values)
            statistics[f'{xpath_expr}-max'] = np.max(values)
        return statistics

    def xpath_count(self, xpath_expr, namespaces):
        """
        Count all elements matching xpath_expr
        """
        values = []
        for e in self.group:
            r = e.xpath(xpath_expr, namespaces=namespaces)
            values += r

        counts = {f'{xpath_expr}-count': len(values)}
        return counts



def sorted_groupby(iterable, key=None):
    """
    Sort iterable by key and then group by the same key.

    itertools.groupby() assumes that the iterable is already sorted. This function
    conveniently sorts the iterable first, and then groups its elements.
    """
    return groupby(sorted(iterable, key=key), key=key)


def _to_dict(root, raise_errors):
    from .mods4pandas import mods_to_dict, mets_to_dict
    from .alto4pandas import alto_to_dict

    root_name = ET.QName(root.tag)
    if root_name.namespace == "http://www.loc.gov/mods/v3":
        return mods_to_dict(root, raise_errors)
    elif root_name.namespace == "http://www.loc.gov/METS/":
        return mets_to_dict(root, raise_errors)
    elif root_name.namespace in [
        "http://schema.ccs-gmbh.com/ALTO",
        "http://www.loc.gov/standards/alto/",
        "http://www.loc.gov/standards/alto/ns-v2#",
        "http://www.loc.gov/standards/alto/ns-v4#",
    ]:
        return alto_to_dict(root, raise_errors)
    else:
        raise ValueError(f"Unknown namespace {root_name.namespace}")


def flatten(d: MutableMapping, parent='', separator='_'):
    """
    Flatten the given nested dict.

    It is assumed that d maps strings to either another dictionary (similarly structured) or some other value.
    """
    items = []

    for k, v in d.items():
        if parent:
            new_key = parent + separator + k
        else:
            new_key = k

        if isinstance(v, MutableMapping):
            items.extend(flatten(v, new_key, separator=separator).items())
        else:
            items.append((new_key, v))

    return dict(items)


def dicts_to_df(data_list: List[Dict], *, index_column: str) -> pd.DataFrame:
    """
    Convert the given list of dicts to a Pandas DataFrame.

    The keys of the dicts make the columns.
    """

    # Build columns from keys
    columns = []
    for m in data_list:
        for c in m.keys():
            if c not in columns:
                columns.append(c)

    # Build data table
    data = [[m.get(c) for c in columns] for m in data_list]

    # Build index
    index = [m[index_column] for m in data_list]

    df = pd.DataFrame(data=data, index=index, columns=columns)
    return df
