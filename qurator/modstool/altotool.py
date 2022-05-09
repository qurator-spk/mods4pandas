#!/usr/bin/env python3
import csv
import logging
import os
import re
import warnings
import sys
from lxml import etree as ET
from itertools import groupby
from operator import attrgetter
from typing import List
from collections.abc import MutableMapping, Sequence

import click
import pandas as pd
from tqdm import tqdm

from .lib import TagGroup, sorted_groupby, flatten, ns


logger = logging.getLogger('altotool')



def alto_to_dict(alto, raise_errors=True):
    """Convert ALTO metadata to a nested dictionary"""

    value = {}

    # Iterate through each group of tags
    for tag, group in sorted_groupby(alto, key=attrgetter('tag')):
        group = list(group)

        # XXX Namespaces seem to use a trailing / sometimes, sometimes not.
        #     (e.g. {http://www.loc.gov/METS/} vs {http://www.loc.gov/METS})
        if tag == '{http://www.loc.gov/standards/alto/ns-v2#}Description':
            value['Description'] = TagGroup(tag, group).is_singleton().has_no_attributes().descend(raise_errors)
        elif tag == '{http://www.loc.gov/standards/alto/ns-v2#}MeasurementUnit':
            value['MeasurementUnit'] = TagGroup(tag, group).is_singleton().has_no_attributes().text()
        elif tag == '{http://www.loc.gov/standards/alto/ns-v2#}OCRProcessing':
            value['OCRProcessing'] = TagGroup(tag, group).is_singleton().descend(raise_errors)
        elif tag == '{http://www.loc.gov/standards/alto/ns-v2#}ocrProcessingStep':
            for n, e in enumerate(group):
                value['ocrProcessingStep{}'.format(n)] = alto_to_dict(e, raise_errors)
        elif tag == '{http://www.loc.gov/standards/alto/ns-v2#}processingDateTime':
            value['processingDateTime'] = TagGroup(tag, group).is_singleton().has_no_attributes().text()
        elif tag == '{http://www.loc.gov/standards/alto/ns-v2#}processingSoftware':
            value['processingSoftware'] = TagGroup(tag, group).is_singleton().descend(raise_errors)
        elif tag == '{http://www.loc.gov/standards/alto/ns-v2#}softwareCreator':
            value['softwareCreator'] = TagGroup(tag, group).is_singleton().has_no_attributes().text()
        elif tag == '{http://www.loc.gov/standards/alto/ns-v2#}softwareName':
            value['softwareName'] = TagGroup(tag, group).is_singleton().has_no_attributes().text()
        elif tag == '{http://www.loc.gov/standards/alto/ns-v2#}softwareVersion':
            value['softwareVersion'] = TagGroup(tag, group).is_singleton().has_no_attributes().text()
        elif tag == '{http://www.loc.gov/standards/alto/ns-v2#}Layout':
            value['Layout'] = TagGroup(tag, group).is_singleton().has_no_attributes().descend(raise_errors)
        elif tag == '{http://www.loc.gov/standards/alto/ns-v2#}Page':
            value['Page'] = {}
            value['Page'].update(TagGroup(tag, group).is_singleton().attributes())
            value['Page'].update(TagGroup(tag, group).subelement_counts())
        elif tag == '{http://www.loc.gov/standards/alto/ns-v2#}Styles':
            pass
        else:
            if raise_errors:
                print(value)
                raise ValueError('Unknown tag "{}"'.format(tag))
            else:
                pass

    return value



def walk(m):
    # XXX do this in modstool, too
    if os.path.isdir(m):
        logger.info('Scanning directory {}'.format(m))
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

    altotool writes two output files: A pickled pandas DataFrame and a CSV file with all conversion warnings.
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

                    alto_info.append(d)

                    if caught_warnings:
                        # PyCharm thinks caught_warnings is not Iterable:
                        # noinspection PyTypeChecker
                        for caught_warning in caught_warnings:
                            csvwriter.writerow([alto_file, caught_warning.message])
            except Exception as e:
                logger.error('Exception in {}: {}'.format(alto_file, e))
                #import traceback; traceback.print_exc()

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
