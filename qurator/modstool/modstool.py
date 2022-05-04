#!/usr/bin/env python3
import csv
import logging
import os
import re
import warnings
from lxml import etree as ET
from itertools import groupby
from operator import attrgetter
from typing import List
from collections.abc import MutableMapping, Sequence

import click
import pandas as pd
from tqdm import tqdm

from .lib import sorted_groupby, TagGroup, ns



logger = logging.getLogger('modstool')

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
                .force_singleton() \
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
        elif tag == '{http://www.loc.gov/mods/v3}partName':
            value['partName'] = TagGroup(tag, group).is_singleton().has_no_attributes().text()
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
            value["language"] = TagGroup(tag, group) \
                .merge_sub_tags_to_set()
        elif tag == '{http://www.loc.gov/mods/v3}languageTerm':
            value['languageTerm'] = TagGroup(tag, group) \
                .has_attributes({'authority': 'iso639-2b', 'type': 'code'}) \
                .text_set()
        elif tag == '{http://www.loc.gov/mods/v3}scriptTerm':
            value['scriptTerm'] = TagGroup(tag, group) \
                .fix_script_term() \
                .has_attributes({'authority': 'iso15924', 'type': 'code'}) \
                .text_set()
        elif tag == '{http://www.loc.gov/mods/v3}relatedItem':
            pass
        elif tag == '{http://www.loc.gov/mods/v3}name':
            for n, e in enumerate(group):
                value['name{}'.format(n)] = mods_to_dict(e, raise_errors)
        elif tag == '{http://www.loc.gov/mods/v3}role':
            value["role"] = TagGroup(tag, group) \
                .has_no_attributes() \
                .merge_sub_tags_to_set()
        elif tag == '{http://www.loc.gov/mods/v3}roleTerm':
            value['roleTerm'] = TagGroup(tag, group) \
                .has_attributes({'authority': 'marcrelator', 'type': 'code'}) \
                .text_set()
        elif tag == '{http://www.loc.gov/mods/v3}namePart':
            for e in group:
                if not e.attrib.get('type'):
                    value['namePart'] = e.text
                else:
                    value['namePart-{}'.format(e.attrib['type'])] = e.text
        elif tag == '{http://www.loc.gov/mods/v3}nameIdentifier':
            # TODO Use this (e.g. <mods:nameIdentifier type="ppn">106168096</mods:nameIdentifier>) or the
            # mods:name@valueURI to disambiguate
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
                raise ValueError('Unknown tag "{}"'.format(tag))
            else:
                pass

    return value


def mets_to_dict(mets, raise_errors=True):
    """Convert METS metadata to a nested dictionary"""

    # The approach taken here is to handle each element explicitly. This also means that ignored elements are ignored
    # explicitly.

    value = {}

    # Iterate through each group of tags
    for tag, group in sorted_groupby(mets, key=attrgetter('tag')):
        group = list(group)

        # XXX Namespaces seem to use a trailing / sometimes, sometimes not.
        #     (e.g. {http://www.loc.gov/METS/} vs {http://www.loc.gov/METS})
        if tag == '{http://www.loc.gov/METS/}amdSec':
            pass  # TODO
        elif tag == '{http://www.loc.gov/METS/}dmdSec':
            pass  # TODO
        elif tag == '{http://www.loc.gov/METS/}metsHdr':
            pass  # TODO
        elif tag == '{http://www.loc.gov/METS/}structLink':
            pass  # TODO
        elif tag == '{http://www.loc.gov/METS/}structMap':
            pass  # TODO
        elif tag == '{http://www.loc.gov/METS/}fileSec':
            value['fileSec'] = TagGroup(tag, group) \
                .is_singleton().descend(raise_errors)
        elif tag == '{http://www.loc.gov/METS/}fileGrp':
            for e in group:
                use = e.attrib.get('USE')
                if not use:
                    raise ValueError('No USE attribute for fileGrp {}'.format(e))
                value[f'fileGrp-{use}-count'] = len(e)
        else:
            if raise_errors:
                print(value)
                raise ValueError('Unknown tag "{}"'.format(tag))
            else:
                pass

    return value


@click.command()
@click.argument('mets_files', type=click.Path(exists=True), required=True, nargs=-1)
@click.option('--output', '-o', 'output_file', type=click.Path(), help='Output pickle file',
              default='mods_info_df.pkl', show_default=True)
@click.option('--output-csv', type=click.Path(), help='Output CSV file')
@click.option('--output-xlsx', type=click.Path(), help='Output Excel .xlsx file')
def process(mets_files: List[str], output_file: str, output_csv: str, output_xlsx: str):
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
            logger.info('Scanning directory {}'.format(m))
            mets_files_real.extend(f.path for f in tqdm(os.scandir(m), leave=False)
                                   if f.is_file() and not f.name.startswith('.'))
        else:
            mets_files_real.append(m)

    # Process METS files
    with open(output_file + '.warnings.csv', 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        mods_info = []
        logger.info('Processing METS files')
        for mets_file in tqdm(mets_files_real, leave=False):
            try:
                root = ET.parse(mets_file).getroot()
                mets = root # XXX .find('mets:mets', ns) does not work here
                mods = root.find('mets:dmdSec//mods:mods', ns)

                with warnings.catch_warnings(record=True) as caught_warnings:
                    warnings.simplefilter('always')  # do NOT filter double occurrences

                    # MODS
                    d = flatten(mods_to_dict(mods, raise_errors=True))
                    # METS
                    d_mets = flatten(mets_to_dict(mets, raise_errors=True))
                    for k, v in d_mets.items():
                        d[f"mets_{k}"] = v
                    # "meta"
                    d['mets_file'] = mets_file

                    mods_info.append(d)

                    if caught_warnings:
                        # PyCharm thinks caught_warnings is not Iterable:
                        # noinspection PyTypeChecker
                        for caught_warning in caught_warnings:
                            csvwriter.writerow([mets_file, caught_warning.message])
            except Exception as e:
                logger.error('Exception in {}: {}'.format(mets_file, e))
                #import traceback; traceback.print_exc()

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
    logger.info('Writing DataFrame to {}'.format(output_file))
    mods_info_df.to_pickle(output_file)
    if output_csv:
        logger.info('Writing CSV to {}'.format(output_csv))
        mods_info_df.to_csv(output_csv)
    if output_xlsx:
        logger.info('Writing Excel .xlsx to {}'.format(output_xlsx))
        mods_info_df.to_excel(output_xlsx)


def main():
    logging.basicConfig(level=logging.INFO)

    for prefix, uri in ns.items():
        ET.register_namespace(prefix, uri)

    process()


if __name__ == '__main__':
    main()
