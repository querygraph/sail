"""Cases with nothing for a client to observe, kept so each has its reason."""

import pytest

SOURCE_LOCATION = "PipelineEvent carries only timestamp and message, and neither names a Python source location"


@pytest.mark.skip(reason=SOURCE_LOCATION)
def test_failed_flow_progress_event_has_correct_python_source_code_location():
    pass


@pytest.mark.skip(reason=SOURCE_LOCATION)
def test_flow_progress_events_have_correct_python_source_code_location():
    pass


@pytest.mark.skip(reason="No client can make a run end with no cause: both reasons need a flow that fails to stop")
def test_throw_run_failure_surfaces_the_message_when_the_failure_has_no_cause():
    pass
