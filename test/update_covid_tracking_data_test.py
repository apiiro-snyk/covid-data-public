from io import StringIO

import pandas as pd
import pytest
import structlog

from covidactnow.datapublic import common_df
from covidactnow.datapublic.common_fields import CommonFields
from covidactnow.datapublic.common_test_helpers import to_dict

from scripts import update_covid_data_scraper
from scripts import update_covid_tracking_data

from scripts.helpers import UNEXPECTED_COLUMNS_MESSAGE

# turns all warnings into errors for this module
pytestmark = pytest.mark.filterwarnings("error")


def test_transform():
    in_df = common_df.read_csv(
        StringIO(
            "date,state,positive,negative,fips,pending\n"
            "20200401,TX,10,1000,48,\n"
            "20200402,TX,11,1100,48,\n"
        ),
        set_index=False,
    )
    with structlog.testing.capture_logs() as logs:
        out_df = update_covid_tracking_data.transform(in_df)

    expected_df = common_df.read_csv(
        StringIO(
            "date,state,country,aggregate_level,positive_tests,negative_tests,fips\n"
            "2020-04-01,TX,USA,state,10,1000,48\n"
            "2020-04-02,TX,USA,state,11,1100,48\n"
        ),
        set_index=False,
    )

    assert to_dict(["fips", "date"], out_df) == to_dict(["fips", "date"], expected_df)

    assert [l["event"] for l in logs] == [
        UNEXPECTED_COLUMNS_MESSAGE,
    ]
