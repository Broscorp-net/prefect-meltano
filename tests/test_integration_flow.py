from prefect import flow

from prefect_meltano.sink_wrapper import run_singer_target_task
from prefect_meltano.tap_wrapper import run_singer_tap_task
from tests.test_tap_wrapper import CSV_TAP_CONFIG


async def test_integration_flow(tmp_path):
    from tap_csv.tap import TapCSV
    from meltanolabs_target_csv.target import TargetCSV
    @flow
    async def test_flow():
        result = await run_singer_tap_task(tap_ref=TapCSV, config=CSV_TAP_CONFIG, raise_on_error=True)
        return await run_singer_target_task(target_ref=TargetCSV, input_file=result.stdout_file, raise_on_error=True,
                                            config={"file_naming_scheme": "{stream_name}.csv",
                                                    "output_path": str(tmp_path)})

    res = await test_flow()
    assert res.ok
    expected_file = tmp_path / "leads.csv"

    # Verify file exists and content
    assert expected_file.exists()
    content = expected_file.read_text()

    # Check number of lines (header + 3 records)
    assert len(content.splitlines()) == 4

    # Check specific content
    assert "test2" in content
    assert "test3" in content
    assert "Id" in content  # header field
