"""Project pipelines."""
from __future__ import annotations

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline

import hedno.pipelines.preprocess as prp


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from pipeline names to ``Pipeline`` objects.
    """
    preprocess_pipeline = prp.create_pipeline()

    return {
        "__default__": preprocess_pipeline,
        "preprocess": preprocess_pipeline,
    }
