import pytest
from prefect.testing.utilities import prefect_test_harness

from prefect_meltano.tap_wrapper import run_singer_tap_task


from tap_csv.tap import TapCSV

CSV_TAP_CONFIG = {
    "files": [
        {"entity": "leads",
         "path": "tests/leads.csv",
         "keys": ["Id"],
         "delimiter": ";"
         }
    ]
}

CSV_TAP_CONFIG_ERROR = {
    "files": [
        {"entity": "leads",
         "path": "non-existing.csv",
         "keys": ["Id"],
         "delimiter": ","
         }
    ]
}

@pytest.fixture(scope="session", autouse=True)
def prefect_db():
    """
    Sets up test harness for temporary DB during test runs.
    """
    with prefect_test_harness():
        yield


@pytest.mark.asyncio
async def test_run_singer_tap_task_success():
    result = await run_singer_tap_task(tap_ref=TapCSV, config=CSV_TAP_CONFIG)

    assert result.ok is True
    out = result.stdout_text()
    err = result.stderr_text()

    # Captured stdout should contain our prints
    assert "test2" in out
    assert "test3" in out

    # Command representation should mention the tap class
    assert "TapCSV" in result.command_repr

    # Logs should be captured to stderr/log buffer (may include timestamp/level)
    assert '"metric": "record_count", "value": 3' in err


@pytest.mark.asyncio
async def test_run_singer_tap_task_exception_capture_no_reraise():
    # Default raise_on_error=False should capture exception and not raise
    result = await run_singer_tap_task(tap_ref=TapCSV, config=CSV_TAP_CONFIG_ERROR)
    assert result.ok is False
    assert result.exception is not None


@pytest.mark.asyncio
async def test_run_singer_tap_task_exception_reraise():
    # When raise_on_error=True, the task should raise
    with pytest.raises(Exception, match="File path does not exist"):
        await run_singer_tap_task(tap_ref=TapCSV, config=CSV_TAP_CONFIG_ERROR, raise_on_error=True)