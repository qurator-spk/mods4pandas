Extract the MODS/ALTO metadata of a bunch of METS/ALTO files into pandas DataFrames.

[![Build Status](https://circleci.com/gh/qurator-spk/modstool.svg?style=svg)](https://circleci.com/gh/qurator-spk/modstool)

**modstool** converts the MODS metadata from METS files into a pandas DataFrame.

Column names are derived from the corresponding MODS elements. Some domain
knowledge is used to convert elements to a useful column, e.g. produce sets
instead of ordered lists for topics, etc. Parts of the tool are specific to
our environment/needs at the State Library Berlin and may need to be changed for
your library.

**alto4pandas** convets the metadata from ALTO files into a pandas DataFrame.

Column names are derived from the corresponding ALTO elements. Some columns
contain descriptive statistics (e.g. counts or mean) of the corresponding ALTO
elements or attributes.

## Usage
~~~
modstool /path/to/a/directory/containing/mets_files
~~~

~~
alto4pandas /path/to/a/directory/full/of/alto_files
~~~

## Example
In this example we convert the MODS metadata contained in the METS files in
`/srv/data/digisam_mets-sample-300` to a pandas DataFrame under
`mods_info_df.pkl`. This file can then be read by your data scientist using
`pd.read_pickle()`.

~~~
% modstool /srv/data/digisam_mets-sample-300
INFO:root:Scanning directory /srv/data/digisam_mets-sample-300
301it [00:00, 19579.19it/s]
INFO:root:Processing METS files
100%|████████████████████████████████████████| 301/301 [00:01<00:00, 162.59it/s]
INFO:root:Writing DataFrame to mods_info_df.pkl
~~~

In the next example we convert the metadata from the ALTO files in the test data
directory:

~~~
% alto4pandas qurator/modstool/tests/data/alto
Scanning directory qurator/modstool/tests/data/alto
Scanning directory qurator/modstool/tests/data/alto/PPN636777308
Scanning directory qurator/modstool/tests/data/alto/734008031
Scanning directory qurator/modstool/tests/data/alto/PPN895016346
Scanning directory qurator/modstool/tests/data/alto/PPN640992293
Scanning directory qurator/modstool/tests/data/alto/alto-ner
Scanning directory qurator/modstool/tests/data/alto/PPN767883624
Scanning directory qurator/modstool/tests/data/alto/PPN715049151
Scanning directory qurator/modstool/tests/data/alto/749782137
Scanning directory qurator/modstool/tests/data/alto/weird-ns
INFO:alto4pandas:Processing ALTO files
INFO:alto4pandas:Writing DataFrame to alto_info_df.pkl
~~~
