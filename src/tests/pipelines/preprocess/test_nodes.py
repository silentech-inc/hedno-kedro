import unittest

import pandas as pd
from pandas import testing as tm
import pytest

from hedno.pipelines.preprocess import nodes


class TestDatelib(unittest.TestCase):
    def test_parse_hedno_dates(self):
        input_dates = pd.Series([
            "01/02/2010",
            "02-03-2011",
            "03.04.2012",
            "04052013",
            "5/6/14",
        ])

        target_dates = pd.Series([
            "2010-02-01",
            "2011-03-02",
            "2012-04-03",
            "2013-05-04",
            "2014-06-05"
        ]).astype("datetime64[ns]")

        output_dates = nodes.parse_hedno_dates(input_dates)

        tm.assert_series_equal(output_dates, target_dates)
