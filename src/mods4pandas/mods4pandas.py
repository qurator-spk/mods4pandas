#!/usr/bin/env python3
import contextlib
import csv
import logging
import os
import re
import sqlite3
import warnings
import sys
from lxml import etree as ET
from itertools import groupby
from operator import attrgetter
from typing import Dict, List
from collections import defaultdict
from collections.abc import MutableMapping, Sequence

import click
import pandas as pd
from tqdm import tqdm

from .lib import sorted_groupby, TagGroup, ns, flatten, dicts_to_df, insert_into_db, insert_into_db_multiple



logger = logging.getLogger('mods4pandas')

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
            # By default we assume source="gbv-ppn" mods:recordIdentifiers (= PPNs),
            # however, in mods:relatedItems, there may be source="dnb-ppns",
            # which we need to distinguish by using a separate field name.
            try:
                value['recordIdentifier'] = TagGroup(tag, group).is_singleton().has_attributes({'source': 'gbv-ppn'}).text()
            except ValueError:
                value['recordIdentifier-dnb-ppn'] = TagGroup(tag, group).is_singleton().has_attributes({'source': 'dnb-ppn'}).text()
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
            tag_group = TagGroup(tag, group)
            for type_, grouped_group in sorted_groupby(tag_group.group, key=lambda g: g.attrib['type']):
                sub_tag = 'relatedItem-{}'.format(type_)
                grouped_group = list(grouped_group)
                if type_ in ["original", "host"]:
                    value[sub_tag] = TagGroup(sub_tag, grouped_group).is_singleton().descend(raise_errors)
                else:
                    # TODO type="series"
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

def pages_to_dict(mets, raise_errors=True) -> List[Dict]:
    # TODO replace asserts by ValueError

    result = []

    # PPN
    def get_mets_recordIdentifier(*, source="gbv-ppn"):
        return (mets.xpath(f'//mets:dmdSec[1]//mods:mods/mods:recordInfo/mods:recordIdentifier[@source="{source}"]',
                           namespaces=ns) or [None])[0].text
    ppn = get_mets_recordIdentifier()

    # Getting per-page/structure information is a bit different
    structMap_PHYSICAL = mets.find('./mets:structMap[@TYPE="PHYSICAL"]', ns)
    structMap_LOGICAL = mets.find('./mets:structMap[@TYPE="LOGICAL"]', ns)
    fileSec = mets.find('./mets:fileSec', ns)
    if structMap_PHYSICAL is None:
        # This is expected in a multivolume work or periodical!
        if any(
                structMap_LOGICAL.find(f'./mets:div[@TYPE="{t}"]', ns) is not None
                for t in ["multivolume_work", "MultivolumeWork", "multivolume_manuscript", "periodical"]
        ):
            return []
        else:
            raise ValueError("No structMap[@TYPE='PHYSICAL'] found (but not a multivolume work)")
    if structMap_LOGICAL is None:
        raise ValueError("No structMap[@TYPE='LOGICAL'] found")
    if fileSec is None:
        raise ValueError("No fileSec found")

    div_physSequence = structMap_PHYSICAL[0]
    assert div_physSequence.attrib.get("TYPE") == "physSequence"


    # Build a look-up table to get mets:file by @ID
    # This cuts retrieving the mets:file down to half the time.
    mets_file_by_ID = {}
    def _init_mets_file_by_ID():
        for f in fileSec.iterfind('./mets:fileGrp/mets:file', ns):
            mets_file_by_ID[f.attrib.get("ID")] = f
    _init_mets_file_by_ID()

    def get_mets_file(*, ID):
        if ID:
            return mets_file_by_ID[ID]

    def get_mets_div(*, ID):
        if ID:
            return structMap_LOGICAL.findall(f'.//mets:div[@ID="{ID}"]', ns)

    for page in div_physSequence:

        # TODO sort by ORDER?
        assert page.attrib.get("TYPE") == "page"
        page_dict = {}
        page_dict["ppn"] = ppn
        page_dict["ID"] = page.attrib.get("ID")
        for fptr in page:
            assert fptr.tag == "{http://www.loc.gov/METS/}fptr"
            file_id = fptr.attrib.get("FILEID")
            assert file_id

            file_ = get_mets_file(ID=file_id)
            assert file_ is not None
            fileGrp_USE = file_.getparent().attrib.get("USE")
            file_FLocat_href = (file_.xpath('mets:FLocat/@xlink:href', namespaces=ns) or [None])[0]
            page_dict[f"fileGrp_{fileGrp_USE}_file_FLocat_href"] = file_FLocat_href

        def get_struct_log(*, to_phys):
            """
            Get the logical structMap elements that link to the given physical page.

            Keyword arguments:
            to_phys -- ID of the page, as per structMap[@TYPE="PHYSICAL"]
            """

            # This is all XLink, there might be a more generic way to traverse the links. However, currently,
            # it suffices to do this the old-fashioned way.

            sm_links = mets.findall(
                    f'./mets:structLink/mets:smLink[@xlink:to="{to_phys}"]', ns
            )

            targets = []
            for sm_link in sm_links:
                xlink_from = sm_link.attrib.get(f"{{{ns['xlink']}}}from")
                targets.extend(get_mets_div(ID=xlink_from))
            return targets

        struct_divs = set(get_struct_log(to_phys=page_dict["ID"]))

        # In our documents, there are already links to parent elements, but we want to make
        # sure and add them.
        def get_struct_log_parents(div):
            cursor = div
            while (cursor := cursor.getparent()).tag == f"{{{ns['mets']}}}div":
                yield cursor

        struct_divs_to_add = set()
        for struct_div in struct_divs:
            struct_divs_to_add.update(get_struct_log_parents(struct_div))
        struct_divs.update(struct_divs_to_add)

        # Populate structure type indicator variables
        for struct_div in struct_divs:
            type_ = struct_div.attrib.get("TYPE").lower()
            assert type_
            page_dict[f"structMap-LOGICAL_TYPE_{type_}"] = 1

        result.append(page_dict)

    return result


@click.command()
@click.argument('mets_files', type=click.Path(exists=True), required=True, nargs=-1)
@click.option('--output', '-o', 'output_file', type=click.Path(), help='Output Parquet file',
              default='mods_info_df.parquet', show_default=True)
@click.option('--output-page-info', type=click.Path(), help='Output page info Parquet file')
def process(mets_files: List[str], output_file: str, output_page_info: str):
    """
    A tool to convert the MODS metadata in INPUT to a pandas DataFrame.

    INPUT is assumed to be a METS document with MODS metadata. INPUT may optionally be a directory. The tool then reads
    all files in the directory.

    mods4pandas writes two output files: A pandas DataFrame (as Parquet) and a CSV file with all conversion warnings.

    Per-page information (e.g. structure information) can be output to a separate Parquet file.
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


    # Prepare output files
    with contextlib.suppress(FileNotFoundError):
        os.remove(output_file)
    output_file_sqlite3 = output_file + ".sqlite3"
    with contextlib.suppress(FileNotFoundError):
        os.remove(output_file_sqlite3)

    logger.info('Writing SQLite DB to {}'.format(output_file_sqlite3))
    con = sqlite3.connect(output_file_sqlite3)

    if output_page_info:
        output_page_info_sqlite3 = output_page_info + ".sqlite3"
        logger.info('Writing SQLite DB to {}'.format(output_page_info_sqlite3))
        with contextlib.suppress(FileNotFoundError):
            os.remove(output_page_info_sqlite3)
        con_page_info = sqlite3.connect(output_page_info_sqlite3)

    # Process METS files
    with open(output_file + '.warnings.csv', 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        mods_info = []
        page_info = []
        logger.info('Processing METS files')
        for mets_file in tqdm(mets_files_real, leave=True):
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

                    # Save
                    insert_into_db(con, "mods_info", d)
                    con.commit()

                    # METS - per-page
                    if output_page_info:
                        page_info_doc: list[dict] = pages_to_dict(mets, raise_errors=True)
                        insert_into_db_multiple(con_page_info, "page_info", page_info_doc)
                        con_page_info.commit()

                    if caught_warnings:
                        # PyCharm thinks caught_warnings is not Iterable:
                        # noinspection PyTypeChecker
                        for caught_warning in caught_warnings:
                            csvwriter.writerow([mets_file, caught_warning.message])
            except Exception as e:
                logger.exception('Exception in {}'.format(mets_file))

    # Convert the mods_info SQL to a pandas DataFrame
    mods_info_df = pd.read_sql_query("SELECT * FROM mods_info", con, index_col="recordInfo_recordIdentifier")

    # Save the DataFrame
    logger.info('Writing DataFrame to {}'.format(output_file))
    mods_info_df.to_parquet(output_file)

    # Convert page_info
    # TODO
    # if output_page_info:
    #     page_info_df = dicts_to_df(page_info, index_column=("ppn", "ID"))
    #     # Save the DataFrame
    #     logger.info('Writing DataFrame to {}'.format(output_page_info))
    #     page_info_df.to_parquet(output_page_info)


def main():
    logging.basicConfig(level=logging.INFO)

    for prefix, uri in ns.items():
        ET.register_namespace(prefix, uri)

    process()


if __name__ == '__main__':
    main()
