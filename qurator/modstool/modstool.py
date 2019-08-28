#!/usr/bin/env python3
import csv
import logging
import os
import re
import warnings
import xml.etree.ElementTree as ET
from itertools import groupby
from operator import attrgetter
from typing import List
from collections.abc import MutableMapping, Sequence

import click
import pandas as pd
from tqdm import tqdm


ns = {
    'mets': 'http://www.loc.gov/METS/',
    'mods': 'http://www.loc.gov/mods/v3'
}


class TagGroup:
    """Helper class to simplify the parsing and checking of MODS metadata"""

    def __init__(self, tag, group: List[ET.Element]):
        self.tag = tag
        self.group = group

    def __str__(self):
        return '\n'.join(str(ET.tostring(e), 'utf-8').strip() for e in self.group)

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
            t += e.text
        return t

    def text_set(self):
        return {e.text for e in self.group}

    def descend(self, raise_errors):
        return mods_to_dict(self.is_singleton().group[0], raise_errors)

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


def sorted_groupby(iterable, key=None):
    """
    Sort iterable by key and then group by the same key.

    itertools.groupby() assumes that the iterable is already sorted. This function
    conveniently sorts the iterable first, and then groups its elements.
    """
    return groupby(sorted(iterable, key=key), key=key)


def mods_to_dict(mods, raise_errors=True):
    """Convert MODS metadata to a nested dictionary"""

    # The approach taken here is to handle each element explicitly. This also means that ignored elements are ignored
    # explicitly.

    value = {}

    # Iterate through each group of tags
    for tag, group in sorted_groupby(mods, key=attrgetter('tag')):
        group = list(group)
        if tag == '{http://www.loc.gov/mods/v3}location':
            def only_current_location(location):
                return location.get('type') != 'former'
            value['location'] = TagGroup(tag, group) \
                .filter(only_current_location) \
                .has_attributes([{}, {'type': 'current'}]) \
                .is_singleton().descend(raise_errors)
        elif tag == '{http://www.loc.gov/mods/v3}physicalLocation':
            def no_display_label(physical_location):
                return physical_location.get('displayLabel') is None
            value['physicalLocation'] = TagGroup(tag, group).filter(no_display_label).text()
        elif tag == '{http://www.loc.gov/mods/v3}shelfLocator':
            # This element should not be repeated according to MODS-AP 2.3.1, however a few of the files contain
            # a second element with empty text and a "displayLabel" attribute set.
            def no_display_label(shelf_locator):
                return shelf_locator.get('displayLabel') is None
            value['shelfLocator'] = TagGroup(tag, group) \
                .filter(no_display_label) \
                .is_singleton() \
                .has_no_attributes() \
                .text()
        elif tag == '{http://www.loc.gov/mods/v3}originInfo':
            def has_event_type(origin_info):
                # According to MODS-AP 2.3.1, every originInfo should have its eventType set. However, some
                # are empty and not fixable.
                return origin_info.attrib.get('eventType') is not None
            tag_group = TagGroup(tag, group).fix_event_type().filter(has_event_type, warn="has no eventType")
            for event_type, grouped_group in sorted_groupby(tag_group.group, key=lambda g: g.attrib['eventType']):
                for n, e in enumerate(grouped_group):
                    value['originInfo-{}{}'.format(event_type, n)] = mods_to_dict(e, raise_errors)
        elif tag == '{http://www.loc.gov/mods/v3}place':
            value['place'] = TagGroup(tag, group).force_singleton(warn=False).has_no_attributes().descend(raise_errors)
        elif tag == '{http://www.loc.gov/mods/v3}placeTerm':
            value['placeTerm'] = TagGroup(tag, group).is_singleton().has_attributes({'type': 'text'}).text()
        elif tag == '{http://www.loc.gov/mods/v3}dateIssued':
            value['dateIssued'] = TagGroup(tag, group) \
                .fix_date() \
                .sort(key=lambda d: d.attrib.get('keyDate') == 'yes', reverse=True) \
                .ignore_attributes() \
                .force_singleton() \
                .text()
        elif tag == '{http://www.loc.gov/mods/v3}dateCreated':
            value['dateCreated'] = TagGroup(tag, group) \
                .fix_date() \
                .sort(key=lambda d: d.attrib.get('keyDate') == 'yes', reverse=True) \
                .ignore_attributes() \
                .force_singleton() \
                .text()
        elif tag == '{http://www.loc.gov/mods/v3}dateCaptured':
            value['dateCaptured'] = TagGroup(tag, group).fix_date().ignore_attributes().is_singleton().text()
        elif tag == '{http://www.loc.gov/mods/v3}dateOther':
            value['dateOther'] = TagGroup(tag, group).fix_date().ignore_attributes().is_singleton().text()
        elif tag == '{http://www.loc.gov/mods/v3}publisher':
            value['publisher'] = TagGroup(tag, group).force_singleton(warn=False).has_no_attributes().text()
        elif tag == '{http://www.loc.gov/mods/v3}edition':
            value['edition'] = TagGroup(tag, group).force_singleton().has_no_attributes().text()
        elif tag == '{http://www.loc.gov/mods/v3}classification':
            authorities = {e.attrib['authority'] for e in group}
            for authority in authorities:
                sub_group = [e for e in group if e.attrib.get('authority') == authority]
                value['classification-{}'.format(authority)] = TagGroup(tag, sub_group).text_set()
        elif tag == '{http://www.loc.gov/mods/v3}recordInfo':
            value['recordInfo'] = TagGroup(tag, group).is_singleton().has_no_attributes().descend(raise_errors)
        elif tag == '{http://www.loc.gov/mods/v3}recordIdentifier':
            value['recordIdentifier'] = TagGroup(tag, group).is_singleton().has_attributes({'source': 'gbv-ppn'}).text()
        elif tag == '{http://www.loc.gov/mods/v3}identifier':
            for e in group:
                if len(e.attrib) != 1:
                    raise ValueError('Unknown attributes for identifier {}'.format(e.attrib))
                value['identifier-{}'.format(e.attrib['type'])] = e.text
        elif tag == '{http://www.loc.gov/mods/v3}titleInfo':
            def only_standard_title(title_info):
                return title_info.attrib.get('type') is None
            value['titleInfo'] = TagGroup(tag, group) \
                .filter(only_standard_title) \
                .is_singleton().has_no_attributes().descend(raise_errors)
        elif tag == '{http://www.loc.gov/mods/v3}title':
            value['title'] = TagGroup(tag, group).is_singleton().has_no_attributes().text()
        elif tag == '{http://www.loc.gov/mods/v3}subTitle':
            value['subTitle'] = TagGroup(tag, group).force_singleton().has_no_attributes().text()
        elif tag == '{http://www.loc.gov/mods/v3}note':
            # This could be useful if distinguished by type attribute.
            pass
        elif tag == '{http://www.loc.gov/mods/v3}part':
            pass
        elif tag == '{http://www.loc.gov/mods/v3}abstract':
            value['abstract'] = TagGroup(tag, group).has_no_attributes().text()
        elif tag == '{http://www.loc.gov/mods/v3}subject':
            authorities = {e.attrib.get('authority') for e in group}
            for authority in authorities:
                k = 'subject-{}'.format(authority) if authority is not None else 'subject'
                sub_group = [e for e in group if e.attrib.get('authority') == authority]
                value[k] = TagGroup(tag, sub_group).force_singleton().descend(raise_errors)
        elif tag == '{http://www.loc.gov/mods/v3}topic':
            TagGroup(tag, group).text_set()
        elif tag == '{http://www.loc.gov/mods/v3}cartographics':
            pass
        elif tag == '{http://www.loc.gov/mods/v3}geographic':
            TagGroup(tag, group).text_set()
        elif tag == '{http://www.loc.gov/mods/v3}temporal':
            TagGroup(tag, group).text_set()
        elif tag == '{http://www.loc.gov/mods/v3}genre':
            authorities = {e.attrib.get('authority') for e in group}
            for authority in authorities:
                k = 'genre-{}'.format(authority) if authority is not None else 'genre'
                value[k] = {e.text for e in group if e.attrib.get('authority') == authority}
        elif tag == '{http://www.loc.gov/mods/v3}language':
            # Make languageTerm/scriptTerm sets
            sub_dicts = [mods_to_dict(e) for e in group]
            sub_tags = {k for d in sub_dicts for k in d.keys()}
            for sub_tag in sub_tags:
                value['language_{}'.format(sub_tag)] = {d.get(sub_tag) for d in sub_dicts if d.get(sub_tag)}
        elif tag == '{http://www.loc.gov/mods/v3}languageTerm':
            value['languageTerm'] = TagGroup(tag, group) \
                .is_singleton().has_attributes({'authority': 'iso639-2b', 'type': 'code'}) \
                .text()
        elif tag == '{http://www.loc.gov/mods/v3}scriptTerm':
            value['scriptTerm'] = TagGroup(tag, group) \
                .is_singleton() \
                .fix_script_term() \
                .has_attributes({'authority': 'iso15924', 'type': 'code'}) \
                .text()
        elif tag == '{http://www.loc.gov/mods/v3}relatedItem':
            pass
        elif tag == '{http://www.loc.gov/mods/v3}name':
            for n, e in enumerate(group):
                value['name{}'.format(n)] = mods_to_dict(e, raise_errors)
        elif tag == '{http://www.loc.gov/mods/v3}role':
            value['role'] = TagGroup(tag, group).is_singleton().has_no_attributes().descend(raise_errors)
        elif tag == '{http://www.loc.gov/mods/v3}roleTerm':
            value['roleTerm'] = TagGroup(tag, group) \
                .is_singleton().has_attributes({'authority': 'marcrelator', 'type': 'code'}) \
                .text()
        elif tag == '{http://www.loc.gov/mods/v3}namePart':
            pass
        elif tag == '{http://www.loc.gov/mods/v3}displayForm':
            value['displayForm'] = TagGroup(tag, group).is_singleton().has_no_attributes().text()
        elif tag == '{http://www.loc.gov/mods/v3}physicalDescription':
            pass
        elif tag == '{http://www.loc.gov/mods/v3}extension':
            pass
        elif tag == '{http://www.loc.gov/mods/v3}accessCondition':
            for e in group:
                if not e.attrib.get('type'):
                    raise ValueError('Unknown attributes for accessCondition {}'.format(e.attrib))
                value['accessCondition-{}'.format(e.attrib['type'])] = e.text
        elif tag == '{http://www.loc.gov/mods/v3}typeOfResource':
            value['typeOfResource'] = TagGroup(tag, group).is_singleton().has_no_attributes().text()
        elif tag == '{http://www.loc.gov/mods/v3}mods':
            # XXX Ignore nested mods:mods for now (used in mods:subject)
            pass
        else:
            if raise_errors:
                print(value)
                raise ValueError('Unknown tag "{}"'.format(tag))
            else:
                pass

    return value


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


@click.command()
@click.argument('mets_files', type=click.Path(exists=True), required=True, nargs=-1)
@click.option('--output', '-o', 'output_file', type=click.Path(), help='Output pickle file',
              default='mods_info_df.pkl', show_default=True)
def process(mets_files: List[str], output_file: str):
    """
    A tool to convert the MODS metadata in INPUT to a pandas DataFrame.

    INPUT is assumed to be a METS document with MODS metadata. INPUT may optionally be a directory. The tool then reads
    all files in the directory.

    modstool writes two output files: A pickled pandas DataFrame and a CSV file with all conversion warnings.
    """

    # Extend file list if directories are given
    mets_files_real = []
    for m in mets_files:
        if os.path.isdir(m):
            logging.info('Scanning directory {}'.format(m))
            mets_files_real.extend(f.path for f in tqdm(os.scandir(m))
                                   if f.is_file() and not f.name.startswith('.'))
        else:
            mets_files_real.append(m)

    # Process METS files
    with open(output_file + '.warnings.csv', 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        mods_info = []
        logging.info('Processing METS files')
        for mets_file in tqdm(mets_files_real):
            try:
                dmd_sec = ET.parse(mets_file).getroot().find('mets:dmdSec', ns)
                mods = dmd_sec.find('.//mods:mods', ns)

                with warnings.catch_warnings(record=True) as caught_warnings:
                    warnings.simplefilter('always')  # do NOT filter double occurrences
                    d = flatten(mods_to_dict(mods, raise_errors=True))
                    d['mets_file'] = mets_file
                    mods_info.append(d)

                    if caught_warnings:
                        # PyCharm thinks caught_warnings is not Iterable:
                        # noinspection PyTypeChecker
                        for caught_warning in caught_warnings:
                            csvwriter.writerow([mets_file, caught_warning.message])
            except Exception as e:
                warnings.warn('Exception in {}:\n{}'.format(mets_file, e))

    # Convert the mods_info List[Dict] to a pandas DataFrame
    columns = []
    for m in mods_info:
        for c in m.keys():
            if c not in columns:
                columns.append(c)
    data = [[m.get(c) for c in columns] for m in mods_info]
    index = [m['recordInfo_recordIdentifier'] for m in mods_info]  # PPN
    mods_info_df = pd.DataFrame(data=data, index=index, columns=columns)

    # Pickle the DataFrame
    logging.info('Writing DataFrame to {}'.format(output_file))
    mods_info_df.to_pickle(output_file)


def main():
    logging.basicConfig(level=logging.INFO)

    for prefix, uri in ns.items():
        ET.register_namespace(prefix, uri)

    process()


if __name__ == '__main__':
    main()
