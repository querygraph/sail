"""Declarative pipeline registration and execution steps."""

from pathlib import Path

import pytest
from pyspark.errors.exceptions.connect import AnalysisException
from pytest_bdd import given, parsers, then, when


@given("a declarative pipeline", target_fixture="spark")
def declarative_pipeline(pipeline, pipeline_spark):
    return pipeline_spark


@given("a new declarative pipeline", target_fixture="pipeline")
def new_declarative_pipeline(pipeline_factory):
    return pipeline_factory()


@given(parsers.parse("pipeline SQL file {name}"))
def pipeline_sql_file(name, docstring, pipeline):
    _, registry, _ = pipeline
    registry.register_sql(docstring, Path(name))


@when("the pipeline runs")
def run_pipeline(pipeline):
    _, _, run = pipeline
    run()


@when(parsers.parse("the pipeline refreshes {names}"))
def refresh_pipeline(names, pipeline):
    _, _, run = pipeline
    run(refresh=[name.strip() for name in names.split(",")])


@when("the pipeline is validated")
def validate_pipeline(pipeline):
    _, _, run = pipeline
    run(dry=True)


@when(parsers.parse("the pipeline fails with {condition}"), target_fixture="pipeline_error")
def run_pipeline_with_error(condition, pipeline):
    _, _, run = pipeline
    with pytest.raises(AnalysisException, match=condition) as error:
        run()
    return error.value


@then(parsers.parse("pipeline error condition is {condition} with SQL state {sql_state}"))
def pipeline_error_condition(pipeline_error, condition, sql_state):
    assert pipeline_error.getCondition() == condition
    assert pipeline_error.getSqlState() == sql_state


@then("pipeline error parameters")
def pipeline_error_parameters(pipeline_error, datatable, pipeline_spark):
    assert datatable[0] == ["parameter", "value"]
    # Qualified identifiers use the session catalog: Sail and Spark have
    # different default catalog names.
    catalog = pipeline_spark.catalog.currentCatalog().replace("`", "``")
    expected = {key: value.replace("<current_catalog>", catalog) for key, value in datatable[1:]}
    assert pipeline_error.getMessageParameters() == expected


@given(parsers.parse("pipeline SQL file {name} fails with {condition}"))
def pipeline_sql_file_with_error(name, condition, docstring, pipeline):
    _, registry, _ = pipeline
    with pytest.raises(AnalysisException, match=condition):
        registry.register_sql(docstring, Path(name))


@then(parsers.parse("pipeline dataset {name} is not published"))
def dataset_not_published(name, pipeline_spark):
    assert not pipeline_spark.catalog.tableExists(name)


@then(parsers.parse("pipeline dataset {name} is published"))
def dataset_published(name, pipeline_spark):
    assert pipeline_spark.catalog.tableExists(name)
