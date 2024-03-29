#!/usr/bin/env python3
import csv
import logging
import os
import re
import warnings
import sys
from xml.dom.expatbuilder import Namespaces
from lxml import etree as ET
from itertools import groupby
from operator import attrgetter
from typing import List
from collections.abc import MutableMapping, Sequence

import click
import pandas as pd
import numpy as np
from tqdm import tqdm

from .lib import TagGroup, sorted_groupby, flatten, ns


logger = logging.getLogger('alto4pandas')



def alto_to_dict(alto, raise_errors=True):
    """Convert ALTO metadata to a nested dictionary"""

    value = {}

    # Iterate through each group of tags
    for tag, group in sorted_groupby(alto, key=attrgetter('tag')):
        group = list(group)

        localname = ET.QName(tag).localname
        alto_namespace = ET.QName(tag).namespace
        namespaces={"alto": alto_namespace}

        if localname == 'Description':
            value[localname] = TagGroup(tag, group).is_singleton().has_no_attributes().descend(raise_errors)
        elif localname == 'MeasurementUnit':
            value[localname] = TagGroup(tag, group).is_singleton().has_no_attributes().text()
        elif localname == 'OCRProcessing':
            value[localname] = TagGroup(tag, group).is_singleton().descend(raise_errors)
        elif localname == 'Processing':
            # TODO This enumerated descent is used more than once, DRY!
            for n, e in enumerate(group):
                value[f'{localname}{n}'] = alto_to_dict(e, raise_errors)
        elif localname == 'ocrProcessingStep':
            for n, e in enumerate(group):
                value[f'{localname}{n}'] = alto_to_dict(e, raise_errors)
        elif localname == 'preProcessingStep':
            for n, e in enumerate(group):
                value[f'{localname}{n}'] = alto_to_dict(e, raise_errors)
        elif localname == 'processingDateTime':
            value[localname] = TagGroup(tag, group).is_singleton().has_no_attributes().text()
        elif localname == 'processingSoftware':
            value[localname] = TagGroup(tag, group).is_singleton().descend(raise_errors)
        elif localname == 'processingAgency':
            value[localname] = TagGroup(tag, group).is_singleton().has_no_attributes().text()
        elif localname == 'processingStepDescription':
            value[localname] = TagGroup(tag, group).is_singleton().has_no_attributes().text()
        elif localname == 'processingStepSettings':
            value[localname] = TagGroup(tag, group).is_singleton().has_no_attributes().text()
        elif localname == 'softwareCreator':
            value[localname] = TagGroup(tag, group).is_singleton().has_no_attributes().text()
        elif localname == 'softwareName':
            value[localname] = TagGroup(tag, group).is_singleton().has_no_attributes().text()
        elif localname == 'softwareVersion':
            value[localname] = TagGroup(tag, group).is_singleton().has_no_attributes().text()

        elif localname == 'sourceImageInformation':
            value[localname] = TagGroup(tag, group).is_singleton().has_no_attributes().descend(raise_errors)
        elif localname == 'fileName':
            value[localname] = TagGroup(tag, group).is_singleton().has_no_attributes().text()

        elif localname == 'Layout':
            value[localname] = TagGroup(tag, group).is_singleton().has_no_attributes().descend(raise_errors)
        elif localname == 'Page':
            value[localname] = {}
            value[localname].update(TagGroup(tag, group).is_singleton().attributes())
            value[localname].update(TagGroup(tag, group).subelement_counts())
            value[localname].update(TagGroup(tag, group).xpath_statistics("//alto:String/@WC", namespaces))

            # Count all alto:String elements with TAGREFS attribute
            value[localname].update(TagGroup(tag, group).xpath_count("//alto:String[@TAGREFS]", namespaces))

        elif localname == 'Styles':
            pass
        elif localname == 'Tags':
            value[localname] = {}
            value[localname].update(TagGroup(tag, group).subelement_counts())
        else:
            if raise_errors:
                print(value)
                raise ValueError('Unknown tag "{}"'.format(tag))
            else:
                pass

    return value



def walk(m):
    # XXX do this in mods4pandas, too
    if os.path.isdir(m):
        tqdm.write(f'Scanning directory {m}')
        for f in tqdm(os.scandir(m), leave=False):
            if f.is_file() and not f.name.startswith('.'):
                yield f.path
            elif f.is_dir():
                try:
                    yield from walk(f.path)
                except PermissionError:
                    warnings.warn(f"Error walking {f.path}")
    else:
        yield m.path



@click.command()
@click.argument('alto_files', type=click.Path(exists=True), required=True, nargs=-1)
@click.option('--output', '-o', 'output_file', type=click.Path(), help='Output pickle file',
              default='alto_info_df.pkl', show_default=True)
@click.option('--output-csv', type=click.Path(), help='Output CSV file')
@click.option('--output-xlsx', type=click.Path(), help='Output Excel .xlsx file')
def process(alto_files: List[str], output_file: str, output_csv: str, output_xlsx: str):
    """
    A tool to convert the ALTO metadata in INPUT to a pandas DataFrame.

    INPUT is assumed to be a ALTO document. INPUT may optionally be a directory. The tool then reads
    all files in the directory.

    alto4pandas writes two output files: A pickled pandas DataFrame and a CSV file with all conversion warnings.
    """

    # Extend file list if directories are given
    alto_files_real = []
    for m in alto_files:
        for x in walk(m):
            alto_files_real.append(x)

    # Process ALTO files
    with open(output_file + '.warnings.csv', 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        alto_info = []
        logger.info('Processing ALTO files')
        for alto_file in tqdm(alto_files_real, leave=False):
            try:
                root = ET.parse(alto_file).getroot()
                alto = root # XXX .find('alto:alto', ns) does not work here

                with warnings.catch_warnings(record=True) as caught_warnings:
                    warnings.simplefilter('always')  # do NOT filter double occurrences

                    # ALTO
                    d = flatten(alto_to_dict(alto, raise_errors=True))
                    # "meta"
                    d['alto_file'] = alto_file
                    d['alto_xmlns'] = ET.QName(alto).namespace

                    alto_info.append(d)

                    if caught_warnings:
                        # PyCharm thinks caught_warnings is not Iterable:
                        # noinspection PyTypeChecker
                        for caught_warning in caught_warnings:
                            csvwriter.writerow([alto_file, caught_warning.message])
            except Exception as e:
                logger.error('Exception in {}: {}'.format(alto_file, e))
                import traceback; traceback.print_exc()

    # Convert the alto_info List[Dict] to a pandas DataFrame
    columns = []
    for m in alto_info:
        for c in m.keys():
            if c not in columns:
                columns.append(c)
    data = [[m.get(c) for c in columns] for m in alto_info]
    index = [m['alto_file'] for m in alto_info] # TODO use ppn + page?
    alto_info_df = pd.DataFrame(data=data, index=index, columns=columns)

    # Pickle the DataFrame
    logger.info('Writing DataFrame to {}'.format(output_file))
    alto_info_df.to_pickle(output_file)
    if output_csv:
        logger.info('Writing CSV to {}'.format(output_csv))
        alto_info_df.to_csv(output_csv)
    if output_xlsx:
        logger.info('Writing Excel .xlsx to {}'.format(output_xlsx))
        alto_info_df.to_excel(output_xlsx)


def main():
    logging.basicConfig(level=logging.INFO)

    for prefix, uri in ns.items():
        ET.register_namespace(prefix, uri)

    process()


if __name__ == '__main__':
    main()
