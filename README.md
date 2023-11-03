# Kedro demo for HEDNO Datathon

## Overview

This is a small, open-source demo of our codebase for the 2nd phase of the HEDNO Datathon. It is meant to showcase [Kedro](https://kedro.org/) as a framework for developing, visualising and deploying Machine Learning projects.

Included in the demo is one Data Engineering pipeline, which loads the CSV files provided by DEDDIE and processes them into a collection of tables which form a proper ontology. The resulting collection has a significantly smaller storage footprint, properly parsed data types (especially dates), and short, clear column names. We store files in [Feather](https://arrow.apache.org/docs/python/feather.html) format.

The pipeline can be run in "production mode" (using `kedro run --pipeline preprocess`) or in "demonstration/debug mode", by executing the provided Jupyter notebook. Either way will result in the same output data appearing. To ensure this flexibility, coding conventions are in place: all code which forms the "backbone" of data engineering resides in `src/hedno/pipelines/preprocess`: `nodes.py` contains functions implementing data-processing tasks, which are organised as a DAG in `pipeline.py`. The notebook invokes code from `nodes.py` and contains additional code for EDA purposes. The pipeline can be visualised in an interactive chart using `kedro viz`

## Rules and guidelines

In order to get the best out of the template:

* Don't remove any lines from the `.gitignore` file we provide
* Make sure your results can be reproduced by following a data engineering convention
* Don't commit data to your repository
* Don't commit any credentials or your local configuration to your repository. Keep all your credentials and local configuration in `conf/local/`

## How to install dependencies

Declare any dependencies in `src/requirements.txt` for `pip` installation and `src/environment.yml` for `conda` installation.

To install them, run:

```
pip install -r src/requirements.txt
```

## How to run your Kedro pipeline

You can run your Kedro project with:

```
kedro run
```

## How to test your Kedro project

Have a look at the file `src/tests/test_run.py` for instructions on how to write your tests. You can run your tests as follows:

```
kedro test
```

To configure the coverage threshold, go to the `.coveragerc` file.

## Explore the code using JupyterLab

We recommend using [JupyterLab](https://jupyter.org/) to explore the notebooks. JupyterLab is already included in the project requirements by default, so once you have run `pip install -r src/requirements.txt` you will not need to take any extra steps before you use them. Note, however, that you need to launch a Kedro-aware kernel:

```
kedro jupyter lab
```

Kedro will automatically define a small number of variables which allow access to the framework: `context`, `catalog` and `startup_error`. The most important of these is `catalog`, through which all datasets can be loaded and saved in a storage-agnostic way.
