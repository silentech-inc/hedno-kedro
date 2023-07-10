"""Preprocess pipeline"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    VOLTAGE_TYPE,

    collect_accounts,
    collect_meters,
    collect_methods,
    collect_places,
    collect_powers,
    collect_rates,
    collect_suppliers,
    collect_uses,

    concat_annotated_train_and_test,
    concat_train_and_test,

    factor_consumptions,
    factor_locations,
    factor_powerthefts,
    factor_records,
    factor_records_and_locations,
    factor_representations,
    factor_requests,
    factor_tests,

    hotfix_powerthefts,
    hotfix_representations_train,
    hotfix_requests_train,

    preprocess_consumptions,
    preprocess_powers,
    preprocess_powerthefts,
    preprocess_rates,
    preprocess_records,
    preprocess_representations,
    preprocess_requests,
    preprocess_suppliers,
    preprocess_tests,
    preprocess_uses,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([

        # HOTFIX
        node(
            func=hotfix_powerthefts,
            inputs="raw_powerthefts",
            outputs="hotfixed_powerthefts",
            name="hotfix_powerthefts",
        ),

        node(
            func=hotfix_representations_train,
            inputs="raw_representations_train",
            outputs="hotfixed_representations_train",
            name="hotfix_representations_train",
        ),

        node(
            func=hotfix_requests_train,
            inputs="raw_requests_train",
            outputs="hotfixed_requests_train",
            name="hotfix_requests_train",
        ),

        # CONCAT
        node(
            func=concat_train_and_test,
            inputs={
                "test_df": "raw_consumptions_test",
                "train_df": "raw_consumptions_train",
            },
            outputs="all_consumptions",
            name="concat_consumptions",
        ),

        node(
            func=concat_annotated_train_and_test,
            inputs={
                "test_df": "raw_records_test",
                "train_df": "raw_records_train"
            },
            outputs="all_records",
            name="concat_records",
        ),

        node(
            func=concat_train_and_test,
            inputs={
                "test_df": "raw_representations_test",
                "train_df": "hotfixed_representations_train",
            },
            outputs="all_representations",
            name="concat_representations",
        ),

        node(
            func=concat_train_and_test,
            inputs={
                "test_df": "raw_requests_test",
                "train_df": "hotfixed_requests_train",
            },
            outputs="all_requests",
            name="concat_requests",
        ),

        # PREPROCESS
        node(
            func=preprocess_consumptions,
            inputs="all_consumptions",
            outputs="preprocessed_consumptions",
            name="preprocess_consumptions",
        ),

        node(
            func=preprocess_powers,
            inputs="raw_powers",
            outputs="preprocessed_powers",
            name="preprocess_powers",
        ),

        node(
            func=preprocess_powerthefts,
            inputs="hotfixed_powerthefts",
            outputs="preprocessed_powerthefts",
            name="preprocess_powerthefts",
        ),

        node(
            func=preprocess_rates,
            inputs="raw_rates",
            outputs="preprocessed_rates",
            name="preprocess_rates",
        ),

        node(
            func=preprocess_records,
            inputs="all_records",
            outputs="preprocessed_records",
            name="preprocess_records",
        ),

        node(
            func=preprocess_representations,
            inputs="all_representations",
            outputs="preprocessed_representations",
            name="preprocess_representations",
        ),

        node(
            func=preprocess_requests,
            inputs="all_requests",
            outputs="preprocessed_requests",
            name="preprocess_requests",
        ),

        node(
            func=preprocess_suppliers,
            inputs="raw_suppliers",
            outputs="preprocessed_suppliers",
            name="preprocess_suppliers",
        ),

        node(
            func=preprocess_tests,
            inputs="raw_tests",
            outputs="preprocessed_tests",
            name="preprocess_tests",
        ),

        node(
            func=preprocess_uses,
            inputs="raw_uses",
            outputs="preprocessed_uses",
            name="preprocess_uses",
        ),

        # COLLECT
        node(
            func=collect_accounts,
            inputs={
                "consumptions": "preprocessed_consumptions",
                "powerthefts": "preprocessed_powerthefts",
                "records": "preprocessed_records",
                "representations": "preprocessed_representations",
                "requests": "preprocessed_requests",
                "tests": "preprocessed_tests"
            },
            outputs="accounts",
            name="collect_accounts",
        ),

        node(
            func=collect_meters,
            inputs={"consumptions": "preprocessed_consumptions"},
            outputs="meters",
            name="collect_meters",
        ),

        node(
            func=collect_methods,
            inputs="preprocessed_powerthefts",
            outputs="methods",
            name="collect_methods",
        ),

        node(
            func=collect_places,
            inputs="preprocessed_powerthefts",
            outputs="places",
            name="collect_places",
        ),

        node(
            func=collect_powers,
            inputs={
                "powers": "preprocessed_powers",
                "records": "preprocessed_records"
            },
            outputs="powers",
            name="collect_powers",
        ),

        node(
            func=collect_rates,
            inputs={
                "consumptions": "preprocessed_consumptions",
                "rates": "preprocessed_rates"
            },
            outputs="rates",
            name="collect_rates",
        ),

        node(
            func=collect_suppliers,
            inputs={
                "representations": "preprocessed_representations",
                "suppliers": "preprocessed_suppliers"
            },
            outputs="suppliers",
            name="collect_suppliers",
        ),

        node(
            func=collect_uses,
            inputs={
                "records": "preprocessed_records", "uses": "preprocessed_uses"
            },
            outputs="uses",
            name="collect_uses",
        ),

        # FACTOR
        node(
            func=factor_consumptions,
            inputs={
                "consumptions": "preprocessed_consumptions",
                "accounts": "accounts",
                "meters": "meters",
                "rates": "rates"
            },
            outputs="factored_consumptions",
            name="factor_consumptions_test",
        ),

        node(
            func=factor_powerthefts,
            inputs={
                "powerthefts": "preprocessed_powerthefts",
                "accounts": "accounts",
                "methods": "methods",
                "places": "places"
            },
            outputs="factored_powerthefts",
            name="factor_powerthefts",
        ),

        node(
            func=factor_locations,
            inputs=["factored_records_and_locations"],
            outputs="factored_locations",
            name="factor_locations",
        ),

        node(
            func=factor_records,
            inputs=["factored_records_and_locations"],
            outputs="factored_records",
            name="factor_records",
        ),

        node(
            func=factor_records_and_locations,
            inputs={
                "accounts": "accounts",
                "records": "preprocessed_records",
                "powers": "powers",
                "uses": "uses"
            },
            outputs="factored_records_and_locations",
            name="factor_records_and_locations",
        ),

        node(
            func=factor_representations,
            inputs={
                "representations": "preprocessed_representations",
                "accounts": "accounts",
                "suppliers": "suppliers"
            },
            outputs="factored_representations",
            name="factor_representations",
        ),

        node(
            func=factor_requests,
            inputs={"requests": "preprocessed_requests", "accounts": "accounts"},
            outputs="factored_requests",
            name="factor_requests",
        ),

        node(
            func=factor_tests,
            inputs={"tests": "preprocessed_tests", "accounts": "accounts"},
            outputs="tests",
            name="factor_tests",
        ),
    ])
