import datetime
import pathlib
import re
from typing import MutableMapping
from typing import Set
from typing import Type

import pandas as pd
import pytz

from covidactnow.datapublic import common_fields

MISSING_COLUMNS_MESSAGE = "DataFrame is missing expected column(s)"
EXTRA_COLUMNS_MESSAGE = "DataFrame has extra unexpected column(s)"


def load_county_fips_data(fips_csv: pathlib.Path) -> pd.DataFrame:
    df = pd.read_csv(fips_csv, dtype={"fips": str})
    df["fips"] = df.fips.str.zfill(5)
    return df


def rename_fields(
    df: pd.DataFrame,
    fields: Type[common_fields.FieldNameAndCommonField],
    already_transformed_fields: Set[str],
    log,
    *,
    check_extra_fields=True,
) -> pd.DataFrame:
    """Return df with columns renamed to common_field names declared in `fields`.

    Unexpected columns are logged. Extra fields are optionally logged and source to add the fields
    to the enum is printed.
    """
    if check_extra_fields:
        extra_fields = set(df.columns) - set(fields) - already_transformed_fields
        if extra_fields:
            # If this warning happens in a test check that the sample data in test/data
            # has the same fields as the argument passed to `fields`.
            log.warning(EXTRA_COLUMNS_MESSAGE, extra_fields=extra_fields)
            print("-- Add the following lines to the appropriate Fields enum --")
            for extra_field in extra_fields:
                enum_name = re.sub(r"(?<!^)(?=[A-Z])", "_", extra_field).upper()
                print(f'    {enum_name} = "{extra_field}", None')
            print("-- end of suggested new Fields --")
    missing_fields = set(fields) - set(df.columns)
    if missing_fields:
        # If this warning happens in a test check that the sample data in test/data
        # has the same fields as the argument passed to `fields`.
        log.warning(MISSING_COLUMNS_MESSAGE, missing_fields=missing_fields)
    rename: MutableMapping[str, str] = {f: f for f in already_transformed_fields}
    for col in df.columns:
        field = fields.get(col)
        if field and field.common_field:
            if field.value in rename:
                raise AssertionError(f"Field {repr(field)} misconfigured")
            rename[field.value] = field.common_field.value
    # Copy only columns in `rename.keys()` to a new DataFrame and rename.
    df = df.loc[:, list(rename.keys())].rename(columns=rename)
    return df


def load_census_state(census_state_path: pathlib.Path) -> pd.DataFrame:
    # By default pandas will parse the numeric values in the STATE column as ints but FIPS are two character codes.
    state_df = pd.read_csv(census_state_path, delimiter="|", dtype={"STATE": str})
    state_df.rename(
        columns={"STUSAB": "state", "STATE": "fips", "STATE_NAME": "state_name"}, inplace=True,
    )
    return state_df


def extract_state_fips(fips: str) -> str:
    """Extracts the state FIPS code from a county or state FIPS code."""
    return fips[:2]


def version_timestamp():
    """Returns a Pacific timezone timestamp for use in version.txt files."""
    pacific = pytz.timezone("US/Pacific")
    d = datetime.datetime.now(pacific)
    return d.strftime("%A %b %d %I:%M:%S %p %Z")
