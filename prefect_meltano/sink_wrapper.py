import asyncio
import importlib
import io
import logging
import tempfile
from typing import Any, Dict, Optional, Type, Union, IO

from prefect import task
from singer_sdk import Target

from prefect_meltano.utils import resolve_class


class SingerTargetResult:
    def __init__(
            self,
            command_repr: str,
            exception: Optional[BaseException] = None,
    ):
        self.command_repr = command_repr
        self.exception = exception

    @property
    def ok(self) -> bool:
        return self.exception is None


def _resolve_target_class(target_ref: Union[str, Type[Target]]) -> Type[Target]:
    if isinstance(target_ref, type) and issubclass(target_ref, Target):
        return target_ref
    if isinstance(target_ref, str):
        module_name: str
        class_name: str
        if ":" in target_ref:
            module_name, class_name = target_ref.split(":", 1)
        else:
            parts = target_ref.split(".")
            if len(parts) < 2:
                raise ValueError(
                    "target_ref must be a Target subclass, 'module:Class', or 'module.Class'"
                )
            module_name = ".".join(parts[:-1])
            class_name = parts[-1]
        mod = importlib.import_module(module_name)
        cls = getattr(mod, class_name)
        if not isinstance(cls, type) or not issubclass(cls, Target):
            raise TypeError(f"{target_ref!r} does not resolve to a singer_sdk.Target subclass")
        return cls
    raise TypeError("target_ref must be a Target subclass or a string import path")


def run_singer_target_from_file(
        target_ref: Union[str, Type[Target]],
        input_file: Union[str, IO[bytes], IO[str], tempfile.SpooledTemporaryFile],
        *,
        config: Optional[Union[Dict[str, Any], str]] = None,
        buffer_max_size: int = 10 * 1024 * 1024,  # 10 MiB
        log_level: int = logging.INFO,
        parse_env_config: bool = True,
        validate_config: bool = True,
        raise_on_error: bool = False,
) -> SingerTargetResult:
    """
    Execute a Singer SDK Target class and feed it records from a file-like input,
    instead of stdin. Captures stdout/stderr into SpooledTemporaryFile.

    Parameters:
      - target_ref: Target class or 'module:Class'/'module.Class' path.
      - input_file: Path or open file-like object containing Singer messages.
                    If binary, will be wrapped with TextIOWrapper (utf-8).
      - config: dict or path to target config.
      - buffer_max_size: Max in-memory bytes before spooling to disk for outputs.
      - log_level: Logging level for singer/target logs.
      - parse_env_config: Allow env var overrides during target init.
      - validate_config: Whether to validate target config on init.
      - raise_on_error: If True, re-raise exceptions after capturing output.

    Returns:
      SingerTargetResult with captured stdout/stderr and any exception raised.
    """
    TargetClass: type[Target] = resolve_class(target_ref, Target)

    close_input_on_exit = False
    text_in: IO[str]
    if isinstance(input_file, str):
        text_in = open(input_file, mode="r", encoding="utf-8", newline="")
        close_input_on_exit = True
    else:
        try:
            input_file.seek(0)
        except Exception:
            pass
        if isinstance(input_file, io.TextIOBase):
            text_in = input_file  # already text
        else:
            text_in = io.TextIOWrapper(input_file, encoding="utf-8")

    cmd_repr_parts = [
        f"target={TargetClass.__module__}.{TargetClass.__name__}",
        f"config={'<dict>' if isinstance(config, dict) else (config or '<none>')}",
        f"input={'<file-like>' if not isinstance(input_file, str) else input_file}",
    ]
    command_repr = "run_singer_target_from_file(" + ", ".join(cmd_repr_parts) + ")"

    exception: Optional[BaseException] = None

    try:
        target = TargetClass(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )
        target.listen(input_file)
    except BaseException as exc:
        exception = exc
        if raise_on_error:
            raise
    finally:
        if close_input_on_exit:
            try:
                text_in.close()
            except Exception:
                pass
        else:
            # If we wrapped a binary file-like, avoid closing underlying buffer
            if isinstance(text_in, io.TextIOWrapper):
                try:
                    text_in.flush()
                    text_in.detach()
                except Exception:
                    pass

    return SingerTargetResult(
        command_repr=command_repr,
        exception=exception,
    )


@task(name="run_singer_target_from_file_task", description="Run singer target reading from a file-like input")
async def run_singer_target_task(*args, **kwargs) -> SingerTargetResult:
    """
    Async Prefect task wrapper that executes run_singer_target_from_file in a thread.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: run_singer_target_from_file(*args, **kwargs))
