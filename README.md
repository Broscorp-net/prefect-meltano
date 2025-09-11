# prefect-meltano
Meltano integration for Prefect

# What? Why?
Since Meltano is more of a complete framework with heavy CLI usage involved we needed
an easy programmable interface to reuse taps and targets from Singer and Meltano library
within our prefect flows.

# Tell me more
We use `tempfile.SpooledTemporaryFile` as a data transfer object instead of singer's stdout-stdin pipes.

## Example usage

```python
from prefect import flow

from prefect_meltano.sink_wrapper import run_singer_target_task
from prefect_meltano.tap_wrapper import run_singer_tap_task

from tap_csv.tap import TapCSV
from meltanolabs_target_csv.target import TargetCSV


@flow
async def test_flow():
    result = await run_singer_tap_task(tap_ref=TapCSV, config=CSV_TAP_CONFIG, raise_on_error=True)
    return await run_singer_target_task(target_ref=TargetCSV, input_file=result.stdout_file, raise_on_error=True,
                                        config={"file_naming_scheme": "{stream_name}.csv",
                                                "output_path": str(tmp_path)})
```

# Notes
This implementation only works with Taps/Targets defined with new Api.
Old singer connectors won't work for now, but this is in roadmap for sure
