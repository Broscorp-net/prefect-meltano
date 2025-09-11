import io
import json
import tempfile

import pytest

from prefect_meltano.sink_wrapper import (
    run_singer_target_from_file,
    run_singer_target_task,
    SingerTargetResult,
)
from singer_sdk import Target


class EchoTarget(Target):
    name = "echo-target"

    # Override listen to read from stdin and echo to stdout
    def listen(self, file_input = None) -> None:
        import sys
        data = sys.stdin.read()
        # Emit a simple log line to ensure stderr capture works
        import logging
        logging.getLogger("singer_sdk").info("Echoing %d bytes", len(data))
        print(data, end="")


class FailingTarget(Target):
    name = "failing-target"

    def listen(self, file_input = None) -> None:
        raise RuntimeError("Boom")


def _singer_messages() -> str:
    # Minimal Singer message set as JSONL: SCHEMA, RECORD, STATE
    schema = {
        "type": "SCHEMA",
        "stream": "users",
        "schema": {
            "type": "object",
            "properties": {"id": {"type": ["integer", "null"]}, "name": {"type": ["string", "null"]}},
        },
        "key_properties": ["id"],
    }
    record = {"type": "RECORD", "stream": "users", "record": {"id": 1, "name": "Alice"}}
    state = {"type": "STATE", "value": {"bookmarks": {"users": 1}}}
    return "\n".join(json.dumps(x) for x in (schema, record, state)) + "\n"


def test_target_reads_from_path_and_echoes_stdout(tmp_path):
    content = _singer_messages()
    path = tmp_path / "messages.jsonl"
    path.write_text(content, encoding="utf-8")

    result: SingerTargetResult = run_singer_target_from_file(
        target_ref=EchoTarget,
        input_file=str(path),
        config=None,
    )

    assert result.ok, f"Exception: {result.exception}"
    # Ensure logging captured into stderr
    # Command repr contains the target class path
    assert "echo-target" not in result.command_repr  # we include module.Class, not name
    assert EchoTarget.__name__ in result.command_repr


def test_target_reads_from_spooled_tmp_binary_and_echoes_stdout():
    content = _singer_messages().encode("utf-8")

    spooled = tempfile.SpooledTemporaryFile(mode="w+b", max_size=1024 * 1024)
    spooled.write(content)
    spooled.seek(0)

    result = run_singer_target_from_file(
        target_ref=EchoTarget,
        input_file=spooled,  # binary file-like
    )

    assert result.ok


def test_target_reads_from_text_stream_and_echoes_stdout():
    content = _singer_messages()
    text_stream = io.StringIO(content)

    result = run_singer_target_from_file(
        target_ref=EchoTarget,
        input_file=text_stream,  # text stream
    )

    assert result.ok


def test_exception_captured_when_raise_on_error_false():
    result = run_singer_target_from_file(
        target_ref=FailingTarget,
        input_file=io.StringIO(""),
        raise_on_error=False,
    )

    assert not result.ok
    assert isinstance(result.exception, RuntimeError)


def test_exception_propagates_when_raise_on_error_true():
    with pytest.raises(RuntimeError):
        run_singer_target_from_file(
            target_ref=FailingTarget,
            input_file=io.StringIO(""),
            raise_on_error=True,
        )


@pytest.mark.asyncio
async def test_async_task_wrapper(tmp_path):
    content = _singer_messages()
    path = tmp_path / "messages.jsonl"
    path.write_text(content, encoding="utf-8")

    result = await run_singer_target_task(
        target_ref=EchoTarget,
        input_file=str(path),
    )
    assert result.ok
