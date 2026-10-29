"""Declarative pipeline registration and execution steps."""

from pathlib import Path

import pytest
from pyspark.errors.exceptions.connect import AnalysisException
from pytest_bdd import given, parsers, then, when


@given("a declarative pipeline", target_fixture="spark")
def declarative_pipeline(pipeline, pipeline_spark):
    return pipeline_spark


@given(parsers.parse("pipeline SQL file {name}"))
def pipeline_sql_file(name, docstring, pipeline):
    _, registry, _ = pipeline
    registry.register_sql(docstring, Path(name))


@when("the pipeline runs")
def run_pipeline(pipeline):
    _, _, run = pipeline
    run()


@when("the pipeline is validated")
def validate_pipeline(pipeline):
    _, _, run = pipeline
    run(dry=True)


@when(parsers.parse("the pipeline fails with {condition}"))
def run_pipeline_with_error(condition, pipeline):
    _, _, run = pipeline
    with pytest.raises(AnalysisException, match=condition):
        run()


@when(parsers.parse("pipeline validation fails with {condition}"))
def validate_pipeline_with_error(condition, pipeline):
    _, _, run = pipeline
    with pytest.raises(AnalysisException, match=condition):
        run(dry=True)


@given(parsers.parse("pipeline SQL file {name} fails with {condition}"))
def pipeline_sql_file_with_error(name, condition, docstring, pipeline):
    _, registry, _ = pipeline
    with pytest.raises(AnalysisException, match=condition):
        registry.register_sql(docstring, Path(name))


@then(parsers.parse("pipeline dataset {name} is not published"))
def dataset_not_published(name, pipeline_spark):
    assert not pipeline_spark.catalog.tableExists(name)
