Extract the MODS metadata of a bunch of METS files into a pandas DataFrame.

[![Build Status](https://travis-ci.org/qurator-spk/modstool.svg?branch=master)](https://travis-ci.org/qurator-spk/modstool)

Column names are derived from the corresponding MODS elements. Some domain
knowledge is used to convert elements to a useful column, e.g. produce sets
instead of ordered lists for topics, etc. Parts of the tool are specific to
our environment/needs at the State Library Berlin and may need to be changed for
your library.


## Usage
~~~
modstool /path/to/a/directory/containing/mets_files
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
