import pandas as pd
import re


# Fix
mods_info = pd.read_parquet("mods_info_df.parquet")
for c in mods_info.columns:
    if c.endswith("-count"):
        mods_info[c] = mods_info[c].astype('Int64')


# Tmp to parquet
mods_info.to_parquet("tmp.parquet")
mods_info = pd.read_parquet("tmp.parquet")


# Check
EXPECTED_TYPES = {
        r"mets_file": ("object", ["str"]),
        r"titleInfo_title": ("object", ["str"]),
        r"titleInfo_subTitle": ("object", ["str", "NoneType"]),
        r"titleInfo_partName": ("object", ["str", "NoneType"]),
        r"identifier-.*": ("object", ["str", "NoneType"]),
        r"location_.*": ("object", ["str", "NoneType"]),
        r"name\d+_.*": ("object", ["str", "NoneType"]),
        r"relatedItem-.*_recordInfo_recordIdentifier": ("object", ["str", "NoneType"]),
        r".*-count": ("Int64", None),
        r"typeOfResource": ("object", ["str", "NoneType"]),

        # XXX possibly sets:
        r"genre-.*": ("object", ["str", "NoneType"]),
        r"subject-.*": ("object", ["str", "NoneType"]),
        r"language_.*Term": ("object", ["str", "NoneType"]),
        r"classification-.*": ("object", ["str", "NoneType"]),
}
def expected_types(c):
    for r, types in EXPECTED_TYPES.items():
        if re.fullmatch(r, c):
            edt = types[0]
            einner_types = types[1]
            if einner_types:
                einner_types = set(einner_types)
            return edt, einner_types
    return None, None

for c in mods_info.columns:
    dt = mods_info.dtypes[c]
    edt, einner_types = expected_types(c)

    if edt is None:
        print(f"No expected dtype known for column {c}")
    elif dt != edt:
        print(f"Unexpected dtype {dt} for column {c} (expected {edt})")

    if edt == "object":
        inner_types = set(type(v).__name__ for v in mods_info[c])
        if any(it not in einner_types for it in inner_types):
            print(f"Unexpected inner types {inner_types} for column {c} (expected {einner_types})")

