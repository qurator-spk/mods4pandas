Extract the MODS/ALTO metadata of a bunch of METS/ALTO files into pandas DataFrames.

[![Tests](https://github.com/qurator-spk/mods4pandas/actions/workflows/test.yml/badge.svg)](https://github.com/qurator-spk/mods4pandas/actions?query=workflow:"test")
[![License](https://img.shields.io/badge/License-Apache-blue)](#license)
[![Issues](https://img.shields.io/github/issues/qurator-spk/mods4pandas)](https://github.com/qurator-spk/mods4pandas/issues)

**mods4pandas** converts the MODS metadata from METS files into a pandas DataFrame.

Column names are derived from the corresponding MODS elements. Some domain
knowledge is used to convert elements to a useful column, e.g. produce sets
instead of ordered lists for topics, etc. Parts of the tool are specific to
our environment/needs at the State Library Berlin and may need to be changed for
your library.

Per-page information (e.g. structure information from the METS structMap) can
be converted as well (`--output-page-info`).

**alto4pandas** converts the metadata from ALTO files into a pandas DataFrame.

Column names are derived from the corresponding ALTO elements. Some columns
contain descriptive statistics (e.g. counts or mean) of the corresponding ALTO
elements or attributes.

## Usage
~~~
mods4pandas /path/to/a/directory/containing/mets_files
~~~

~~~
alto4pandas /path/to/a/directory/full/of/alto_files
~~~

### Conversion to other formats

CSV:
```
python -c 'import pandas as pd; pd.read_parquet("mods_info_df.parquet").to_csv("mods_info_df.csv")'
```
Excel (requires `XlsxWriter`):
```
python -c 'import pandas as pd; pd.read_parquet("mods_info_df.parquet").to_excel("mods_info_df.xlsx"
, engine="xlsxwriter")'
```

## Example
In this example we convert the MODS metadata contained in the METS files in
`/srv/data/digisam_mets-sample-300` to a pandas DataFrame under
`mods_info_df.parquet`. This file can then be read by your data scientist using
`pd.read_parquet()`.

```
% mods4pandas /srv/data/digisam_mets-sample-300
INFO:root:Scanning directory /srv/data/digisam_mets-sample-300
301it [00:00, 19579.19it/s]
INFO:root:Processing METS files
100%|████████████████████████████████████████| 301/301 [00:01<00:00, 162.59it/s]
INFO:root:Writing DataFrame to mods_info_df.parquet
```

In the next example we convert the metadata from the ALTO files in the test data
directory:

~~~
% alto4pandas qurator/mods4pandas/tests/data/alto
Scanning directory qurator/mods4pandas/tests/data/alto
Scanning directory qurator/mods4pandas/tests/data/alto/PPN636777308
Scanning directory qurator/mods4pandas/tests/data/alto/734008031
Scanning directory qurator/mods4pandas/tests/data/alto/PPN895016346
Scanning directory qurator/mods4pandas/tests/data/alto/PPN640992293
Scanning directory qurator/mods4pandas/tests/data/alto/alto-ner
Scanning directory qurator/mods4pandas/tests/data/alto/PPN767883624
Scanning directory qurator/mods4pandas/tests/data/alto/PPN715049151
Scanning directory qurator/mods4pandas/tests/data/alto/749782137
Scanning directory qurator/mods4pandas/tests/data/alto/weird-ns
INFO:alto4pandas:Processing ALTO files
INFO:alto4pandas:Writing DataFrame to alto_info_df.parquet
~~~
