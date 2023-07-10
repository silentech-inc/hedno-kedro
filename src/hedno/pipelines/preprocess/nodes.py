import datetime

import numpy as np
import pandas as pd


def _replace_nondigit(s: pd.Series, c="-") -> pd.Series:
    """Replace any non-digit with `c`."""
    return s.str.replace(r"[^0-9]", c, regex=True)


def _extract_date_components(s: pd.Series) -> pd.Series:
    """
    Extract year, month and day with regular expressions from each entry of `s`.

    Assumes that only separator is "-"
    """
    return s.str.extract(
        r"(?P<day>\d{1,2})\-?(?P<month>\d{1,2})\-?(?P<year>\d{2,4})"
    )


def parse_hedno_dates(s: pd.Series) -> pd.Series:
    """
    Repair and parse PTF dates of mixed formats.

    Returns
    -------
    Series
        All dates formatted: YYYY-MM-DD
    """
    return (
        s
        .pipe(_replace_nondigit)
        .pipe(_extract_date_components)
        .astype("int64", copy=False)

        # Upgrade 2-digit year to 4-digit year.
        .assign(
            year=lambda df: np.where(
                df["year"] < 100,
                df["year"] + 2000,
                df["year"]
            )
        )

        .pipe(
            pd.to_datetime,
            cache=False,
            errors="raise",
            format="%Y-%m-%d"
        )
    )


def parse_powertheft_dates(powerthefts: pd.DataFrame) -> pd.DataFrame:
    DATE_COLUMNS = ("date", "confirmdate")
    for dc in DATE_COLUMNS:
        powerthefts[dc] = parse_hedno_dates(powerthefts[dc])#.dt.date
    return powerthefts


def hotfix_powerthefts(powerthefts: pd.DataFrame) -> pd.DataFrame:
    return (
        powerthefts
        .dropna(
            how="any",
            ignore_index=True,
            subset=["DETECTION_DATE", "INITIAL_DETECTION_DATE"]
        )
    )


def hotfix_representations_train(
    representations_train: pd.DataFrame,
) -> pd.DataFrame:
    return (
        representations_train
        .replace(
            to_replace={
                # Repair year: 1199 -> 1999 (probably typo).
                "START_DATE": {
                    "1199-11-22": "1999-11-22",
                    "1199-12-08": "1999-12-08",
                },

                # Repair supplier: Π -> π (probably typo).
                "SUPPLIER": {"Π": "π"},
                "SUPPLIER_TO": {"Π": "π"},
            }
        )
        .assign(
            START_DATE= lambda df:
                pd.to_datetime(df["START_DATE"], format="%Y-%m-%d")
        )
    )

    return representations_train


def hotfix_requests_train(requests_train: pd.DataFrame) -> pd.DataFrame:
    """Fix (only) row where startdate is subsequent to enddate."""
    requests_train.loc[1806225, "COMPLETION_DATE"] = datetime.datetime(2014, 3, 4)
    return requests_train


def preprocess_consumptions(consumptions: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data for consumptions

    Args:
        powers: Raw data.
    Returns:
        Preprocessed data, ... #TODO
    """
    return (
        consumptions
        .rename(
            columns={
                "BS_RATE": "rate",
                "CSS_MS_HS_USE": "energy",
                "MEASUREMENT_DATE": "date",
                "SUCCESSOR": "successor"
            },
            errors="raise"
        )
    )


PHASE_TYPE = pd.CategoricalDtype(
    categories=["Μονοφασική", "Τριφασική"], ordered=True
)


def _extract_power_details(descriptions: pd.Series) -> pd.DataFrame:
    return (
        descriptions
        .str
        .extract(r"(?P<phase>Μονοφασική|Τριφασική) (?P<power>\d*).*")
        .replace({"power": ""}, value=pd.NA)
    )


def preprocess_powers(powers: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data for powers.

    Args:
        powers: Raw data.
    Returns:
        Preprocessed data, with `PARNO` and `DESCRIPTION` renamed to
        `power_name` and `description`, and integer `power_id` added as
        primary key.
    """
    pre_powers = (
        powers
        .rename(
            columns={"PARNO": "power_name", "DESCRIPTION": "description"},
            errors="raise"
        )
        .astype({"description": "string"}, copy=False)
    )

    power_details = _extract_power_details(pre_powers["description"])

    return (
        pd
        .concat(
            [pre_powers, power_details], axis=1, ignore_index=False, join="inner"
        )
        .astype({"phase": PHASE_TYPE, "power": "Int64"}, copy=False)
        .sort_values(by=["phase", "power", "description"], ignore_index=True)
        .reindex(
            columns=[
                "phase",
                "power",
                "description",
                "power_name"
            ]
        )
    )


def preprocess_powerthefts(powerthefts: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data for powerthefts

    Args:
        powers: Raw data.
    Returns:
        Preprocessed data, ... #TODO
    """
    return (
        powerthefts
        .rename(
            columns={
                "DETECTION_DATE": "confirmdate",
                "HMANAF": "refdate",
                "INITIAL_DETECTION_DATE": "date",
                "NON_REGISTERED_ENERGY": "energy",
                "SUCCESSOR": "successor",
                "ΣΗΜΕΙΟ_ΕΝΤΟΠΙΣΜΟΥ": "place",
                "ΤΡΟΠΟΣ_ΕΝΤΟΠΙΣΜΟΥ": "method"
            },
            errors="raise"
        )
        .astype({"method": "string", "place": "string"}, copy=False)
        .drop(columns=["refdate"])
        .pipe(parse_powertheft_dates)
    )


def preprocess_rates(rates: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data for rates.

    Args:
        rates: Raw data.
    Returns:
        Preprocessed data, with `BS_RATE` and `DESCRIPTION` renamed to
        `rate` and `rate_name`.
    """
    return (
        rates
        .rename(
            columns={"BS_RATE": "rate", "DESCRIPTION": "rate_name"},
            errors="raise",
        )
        .sort_values(by="rate", ignore_index=True)
    )


VOLTAGE_TYPE = pd.CategoricalDtype(categories=["LOW", "MED"], ordered=True)


def preprocess_records(records: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data for records.

    Args:
        rates: Raw data.
    Returns:
        Preprocessed data, with `ACCT_CONTROL`, `ACCT_WGS84_X`, `ACCT_WGS84_Y`,
        `CONTRACT_CAPACITY`, `PARNO`, `SUCCESSOR`, `VOLTAGE`, and `XRHSH`
        renamed to `control`, `x`, `y`, `capacity`, `power_name`, `successor`,
        `voltage`, and `use`, respectively. `voltage` is cast as category.
    """
    return (
        records
        .rename(
            columns={
                "ACCT_CONTROL": "control",
                "ACCT_WGS84_X": "x",
                "ACCT_WGS84_Y": "y",
                "CONTRACT_CAPACITY": "capacity",
                "PARNO": "power_name",
                "SUCCESSOR": "successor",
                "VOLTAGE": "voltage",
                "XRHSH": "use"
            },
            errors="raise"
        )
        .astype(
            {
                "voltage": VOLTAGE_TYPE

                # Uses with "*" signaling NA may exist; prevent parsing as int.
                #"use": "string"
            },
            copy=False
        )
    )


def preprocess_representations(representations: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data for representations

    Args:
        powers: Raw data.
    Returns:
        Preprocessed data, with `SUCCESSOR`, `SUPPLIER`, `SUPPLIER_ΤΟ`,
        `START_DATE` and `END_DATE` renamed to `successor`, `supplier`,
        `next_supplier`, `start_date` and `end_date`, respectively.
    """
    return (
        representations
        .rename(
            columns={
                "END_DATE": "enddate",
                "START_DATE": "startdate",
                "SUCCESSOR": "successor",
                "SUPPLIER": "supplier",
                "SUPPLIER_TO": "next_supplier",
            },
            errors="raise",
        )
    )


_REQUESTS = [
    "discon", "newCon", "recon", "reprChange", "reprPause", "unknown"
]
REQUEST_TYPE = pd.CategoricalDtype(categories=_REQUESTS, ordered=False)


def preprocess_requests(requests: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data for requests

    Args:
        powers: Raw data.
    Returns:
        Preprocessed data, with `COMPLETION_DATE`, `COMPL_REQUESTS_STATUS`,
        `REQUEST_DATE`, `REQUEST_TYPE` and `SUCCESSOR` renamed to `end_date`,
        `status`, `start_date`, `type` and `successor`; NULLS in `REQUEST_TYPE`
        have been filled with `unknown`.
    """
    return (
        requests
        .rename(
            columns={
                "COMPLETION_DATE": "enddate",
                "COMPL_REQUEST_STATUS": "status",
                "REQUEST_DATE": "startdate",
                "REQUEST_TYPE": "type",
                "SUCCESSOR": "successor"
            },
            errors="raise",
        )
        .replace({"type": ""}, value="unknown")
        .astype({"type": REQUEST_TYPE}, copy=False)
    )


def preprocess_suppliers(suppliers: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data for suppliers.

    Args:
        powers: Raw data.
    Returns:
        Preprocessed data, with `SUPPLIER` and `SUPPLIER_ΝΑΜΕ` renamed to
        `supplier` and `supplier_name`.
    """
    return (
        suppliers
        .rename(
            columns={"SUPPLIER": "supplier", "SUPPLIER_NAME": "supplier_name"},
            errors="raise",
        )
        .sort_values(by="supplier_name", ignore_index=True, na_position="first")
    )


def preprocess_tests(tests: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data for tests

    Args:
        powers: Raw data.
    Returns:
        Preprocessed data, with `SUCCESSOR` renamed to `successor` and column
        `PREDICTION` dropped.
    """
    return (
        tests
        .drop(columns=["PREDICTION"])
        .rename(columns={"SUCCESSOR": "successor"}, errors="raise")
    )


def preprocess_uses(uses: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data for uses

    Args:
        powers: Raw data.
    Returns:
        Preprocessed data, with `XRHSH` and `DESCRIPTION` renamed to `use` and
        `use_name`.
    """
    return (
        uses
        .rename(
            columns={"XRHSH": "use", "DESCRIPTION": "use_name"},
            errors="raise",
        )
        .sort_values(by=["use"], ignore_index=True)
    )


def collect_accounts(
    consumptions: pd.DataFrame,
    powerthefts: pd.DataFrame,
    records: pd.DataFrame,
    representations: pd.DataFrame,
    requests: pd.DataFrame,
    tests: pd.DataFrame
) -> pd.DataFrame:
    return (
        pd
        .concat(
            [
                consumptions["ACCT_NBR"],
                powerthefts["ACCT_NBR"],
                requests["ACCT_NBR"],
                representations["ACCT_NBR"],
                records["ACCT_NBR"],
                tests["ACCT_NBR"],
            ],
            axis=0,
            ignore_index=True,
        )
        .drop_duplicates(ignore_index=True)
        .sort_values(ignore_index=True)
        .astype("string", copy=False)
        .to_frame()
        .reset_index(names="account_id")
    )


def collect_meters(consumptions: pd.DataFrame) -> pd.DataFrame:
    return (
        consumptions
        ["MS_METER_NBR"]
        .drop_duplicates(ignore_index=True)
        .sort_values(ignore_index=True)
        .to_frame()
        .reset_index(names="meter_id")
    )


def collect_methods(powerthefts: pd.DataFrame) -> pd.DataFrame:
    METHOD_ENG_OF_METHOD = {
        "ΑΓΝΩΣΤΟΣ": "unknown",
        "Μέσω Καταμέτρησης (ΔΑΚ)": "measurer",
        "Μέσω Λοιπών Εργασιών & Συντήρησης": "maintenance",
        "Μέσω Στοχευμένων Ελέγχων": "spot check"
    }

    _, methods = powerthefts["method"].factorize(sort=True)

    methods_eng = methods.map(METHOD_ENG_OF_METHOD, na_action="ignore")

    return (
        pd
        .DataFrame(
            {"method": methods, "method_eng": methods_eng}, dtype="string"
        )
        .reset_index(drop=True)
        .reset_index(names="method_id")
    )


def collect_places(powerthefts: pd.DataFrame) -> pd.DataFrame:
    PLACE_ENG_OF_PLACE = {
        "ΑΓΝΩΣΤΟ": "unknown",
        "ΑΚΡΟΔΕΚΤΕΣ": "connectors",
        "ΑΠΕΥΘΕΙΑΣ  ΑΠΟ ΤΟ ΔΙΚΤΥΟ": "network",
        "ΚΙΒΩΤΙΟ ΔΟΚΙΜΩΝ": "test box",
        "ΜΗΧΑΝΙΣΜΟΣ ΜΕΤΡΗΣΗΣ": "counter"
    }

    _, places = powerthefts["place"].factorize(sort=True)

    places_eng = places.map(PLACE_ENG_OF_PLACE, na_action="ignore")

    return (
        pd
        .DataFrame(
            {"place": places, "place_eng": places_eng}, dtype="string"
        )
        .reset_index(drop=True)
        .reset_index(names="place_id")
    )


def collect_powers(powers: pd.DataFrame, records: pd.DataFrame) -> pd.DataFrame:
    return (
        records
        ["power_name"]
        .drop_duplicates(ignore_index=True)
        .astype("string", copy=False)
        .to_frame("power_name")

        # Drop powers which do not appear in Records.
        .merge(powers, how="left", on="power_name")
        .sort_values(
            by=["power_name", "power"], ignore_index=True, na_position="first"
        )
        .reset_index(names="power_id")
        .reindex(
            columns=[
                "power_id",
                "phase",
                "power",
                "description",
                "power_name"
            ]
        )
    )


def collect_rates(
    consumptions: pd.DataFrame, rates: pd.DataFrame
) -> pd.DataFrame:
    return (
        consumptions
        ["rate"]
        .drop_duplicates(ignore_index=True)
        .astype("string")
        .to_frame("rate")

        # Drop rates which do not appear in Consumptions.
        .merge(rates, how="left", on="rate")
        .sort_values(
            by=["rate_name", "rate"], ignore_index=True, na_position="first"
        )
        .reset_index(names="rate_id")
        .reindex(
            columns=[
                "rate_id",
                "rate_name",
                "rate"
            ]
        )
    )


def collect_suppliers(
    representations: pd.DataFrame, suppliers: pd.DataFrame
) -> pd.DataFrame:
        return (
        pd
        .concat(
            [
                representations["next_supplier"],
                representations["supplier"],
            ],
            axis=0,
            ignore_index=True,
        )
        .drop_duplicates(ignore_index=True)
        .dropna(ignore_index=True)
        .to_frame("supplier")

        # Drop suppliers which do not appear in Representations.
        .merge(suppliers, how="left", on="supplier")
        .sort_values(by="supplier_name", ignore_index=True, na_position="first")

        .reset_index(names="supplier_id")
        .reindex(
            columns=[
                "supplier_id",
                "supplier_name",
                "supplier"
            ]
        )
    )


def collect_uses(records: pd.DataFrame, uses: pd.DataFrame) -> pd.DataFrame:
    return (
        records
        ["use"]
        .drop_duplicates(ignore_index=True)
        .to_frame("use")

        # Drop uses which do not appear in Records.
        .merge(uses, how="left", on="use", sort=True)

        .astype({"use": "string"}, copy=False)
        .reset_index(names="use_id")
        .reindex(
            columns=[
                "use_id",
                "use_name",
                "use"
            ]
        )
    )


def concat_train_and_test(
    test_df: pd.DataFrame, train_df: pd.DataFrame
) -> pd.DataFrame:
    return pd.concat(
        [test_df, train_df], copy=False, ignore_index=True, join="inner"
    )


def concat_annotated_train_and_test(
    test_df: pd.DataFrame, train_df: pd.DataFrame
) -> pd.DataFrame:
    return concat_train_and_test(
        test_df=test_df.assign(test=True), train_df=train_df.assign(test=False)
    )


def factor_consumptions(
    consumptions: pd.DataFrame,
    accounts: pd.DataFrame,
    meters: pd.DataFrame,
    rates: pd.DataFrame
) -> pd.DataFrame:
    return (
        consumptions
        .merge(accounts, how="inner", on="ACCT_NBR")
        .drop(columns=["ACCT_NBR"])
        .merge(meters, how="inner", on="MS_METER_NBR")
        .drop(columns=["MS_METER_NBR"])
        .merge(rates, how="inner", on="rate")
        .drop(columns=["rate", "rate_name"])
        .reindex(
            columns=[
                "account_id",
                "successor",
                "rate_id",
                "meter_id",
                "energy",
                "date",
            ]
        )
    )


def factor_powerthefts(
    powerthefts: pd.DataFrame,
    accounts: pd.DataFrame,
    methods: pd.DataFrame,
    places: pd.DataFrame
) -> pd.DataFrame:
    return (
        powerthefts
        .merge(accounts, how="inner", on="ACCT_NBR")
        .drop(columns=["ACCT_NBR"])
        .merge(methods, how="inner", on="method")
        .drop(columns=["method", "method_eng"])
        .merge(places, how="inner", on="place")
        .drop(columns=["place", "place_eng"])
        .reindex(
            columns=[
                "account_id",
                "successor",
                "method_id",
                "place_id",
                "energy",
                "date",
                "confirmdate",
            ]
        )
    )


def factor_records_and_locations(
    records: pd.DataFrame,
    accounts: pd.DataFrame,
    powers: pd.DataFrame,
    uses: pd.DataFrame
) -> pd.DataFrame:
    return (
        records
        .merge(accounts, how="inner", on="ACCT_NBR")
        .drop(columns=["ACCT_NBR"])
        .merge(powers, how="inner", on="power_name")
        .drop(columns=["description", "phase", "power", "power_name"])
        .merge(uses, how="inner", on="use")
        .drop(columns=["use", "use_name"])
        .reindex(
            columns=[
                "account_id",
                "successor",
                "voltage",
                "capacity",
                "power_id",
                "use_id",
                "control",
                "x",
                "y",
                "test"
            ]
        )
    )


def factor_locations(factored_records_and_locations: pd.DataFrame) -> pd.DataFrame:
    LOCATIONS_UNIQUE_KEY = ["account_id"]
    return (
        factored_records_and_locations
        [[*LOCATIONS_UNIQUE_KEY, "x", "y"]]
        .drop_duplicates(ignore_index=True)
        .sort_values(by=LOCATIONS_UNIQUE_KEY, ignore_index=True)
    )


def factor_records(factored_records_and_locations: pd.DataFrame) -> pd.DataFrame:
    return factored_records_and_locations.drop(columns=["x", "y"])


def factor_representations(
    representations: pd.DataFrame,
    accounts: pd.DataFrame,
    suppliers: pd.DataFrame
) -> pd.DataFrame:
    return (
        representations
        .merge(accounts, how="inner", on="ACCT_NBR")
        .drop(columns=["ACCT_NBR"])

        # Current supplier is asummed to be known.
        .merge(suppliers, how="inner", on="supplier")
        .drop(columns=["supplier"])

        # Next supplier may not be known.
        .merge(
            suppliers,
            how="left",
            left_on="next_supplier",
            right_on="supplier",
            suffixes=('', '_next')
        )
        .drop(columns=["next_supplier"])
        .rename(columns={"supplier_id_next": "next_supplier_id"})
        .astype({"next_supplier_id": "Int64"}, copy=False)

        .reindex(
            columns=[
                "account_id",
                "successor",
                "supplier_id",
                "next_supplier_id",
                "startdate",
                "enddate",
            ]
        )
    )


def factor_requests(
    requests: pd.DataFrame, accounts: pd.DataFrame
) -> pd.DataFrame:
    return (
        requests
        .merge(accounts, how="inner", on="ACCT_NBR")
        .drop(columns=["ACCT_NBR"])
        .reindex(
            columns=[
                "account_id",
                "successor",
                "type",
                "status",
                "startdate",
                "enddate",
            ]
        )
    )


def factor_tests(
    tests: pd.DataFrame,
    accounts: pd.DataFrame
) -> pd.DataFrame:
    return (
        tests
        .merge(accounts, how="left", on="ACCT_NBR", sort=True)
        .drop(columns=["ACCT_NBR"])
        .reindex(columns=["account_id", "successor"])
    )
