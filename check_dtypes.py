import re
import warnings
import os

with warnings.catch_warnings():
    # Filter warnings on WSL
    if "Microsoft" in os.uname().release:
        warnings.simplefilter("ignore")
    import pandas as pd


mods_info = pd.read_parquet("mods_info_df.parquet")
alto_info = pd.read_parquet("alto_info_df.parquet")

# Check
EXPECTED_TYPES = {

        # mods_info

        r"mets_file": ("object", ["str"]),
        r"titleInfo_title": ("object", ["str"]),
        r"titleInfo_subTitle": ("object", ["str", "NoneType"]),
        r"titleInfo_partName": ("object", ["str", "NoneType"]),
        r"identifier-.*": ("object", ["str", "NoneType"]),
        r"location_.*": ("object", ["str", "NoneType"]),
        r"name\d+_.*": ("object", ["str", "NoneType"]),
        r"relatedItem-.*_recordInfo_recordIdentifier": ("object", ["str", "NoneType"]),
        r"typeOfResource": ("object", ["str", "NoneType"]),
        r"accessCondition-.*": ("object", ["str", "NoneType"]),
        r"originInfo-.*": ("object", ["str", "NoneType"]),

        r".*-count": ("Int64", None),

        # XXX possibly sets:
        r"genre-.*": ("object", ["str", "NoneType"]),
        r"subject-.*": ("object", ["str", "NoneType"]),
        r"language_.*Term": ("object", ["str", "NoneType"]),
        r"classification-.*": ("object", ["str", "NoneType"]),

        # alto_info

        r"Description_.*": ("object", ["str", "NoneType"]),
        r"Layout_Page_ID": ("object", ["str", "NoneType"]),
        r"Layout_Page_PHYSICAL_(IMG|IMAGE)_NR": ("object", ["str", "NoneType"]),
        r"Layout_Page_PROCESSING": ("object", ["str", "NoneType"]),
        r"Layout_Page_QUALITY": ("object", ["str", "NoneType"]),
        r"Layout_Page_//alto:String/@WC-.*": ("Float64", None),
        r"alto_xmlns": ("object", ["str", "NoneType"]),

        # XXX r"Layout_Page_(WIDTH|HEIGHT)": ("Int64", None),
        r"Layout_Page_(WIDTH|HEIGHT)": ("object", ["str", "NoneType"]),
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

def check_types(df):
    for c in df.columns:
        dt = df.dtypes[c]
        edt, einner_types = expected_types(c)

        if edt is None:
            print(f"No expected dtype known for column {c}")
        elif dt != edt:
            print(f"Unexpected dtype {dt} for column {c} (expected {edt})")

        if edt == "object":
            inner_types = set(type(v).__name__ for v in df[c])
            if any(it not in einner_types for it in inner_types):
                print(f"Unexpected inner types {inner_types} for column {c} (expected {einner_types})")

check_types(mods_info)
check_types(alto_info)

