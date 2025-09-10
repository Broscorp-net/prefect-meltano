import asyncio
import importlib
import io
import logging
import tempfile

from contextlib import redirect_stdout, redirect_stderr
from typing import Any, Dict, Optional, Type, Union

from prefect import task
from singer_sdk import Tap


class SingerTapResult:
    def __init__(
            self,
            stdout_file: tempfile.SpooledTemporaryFile,
            stderr_file: tempfile.SpooledTemporaryFile,
            command_repr: str,
            exception: Optional[BaseException] = None,
    ):
        self.stdout_file = stdout_file
        self.stderr_file = stderr_file
        self.command_repr = command_repr
        self.exception = exception

    def stdout_bytes(self) -> bytes:
        pos = self.stdout_file.tell()
        try:
            self.stdout_file.seek(0)
            return self.stdout_file.read()
        finally:
            self.stdout_file.seek(pos)

    def stderr_bytes(self) -> bytes:
        pos = self.stderr_file.tell()
        try:
            self.stderr_file.seek(0)
            return self.stderr_file.read()
        finally:
            self.stderr_file.seek(pos)

    def stdout_text(self, encoding: str = "utf-8", errors: str = "replace") -> str:
        return self.stdout_bytes().decode(encoding, errors)

    def stderr_text(self, encoding: str = "utf-8", errors: str = "replace") -> str:
        return self.stderr_bytes().decode(encoding, errors)

    @property
    def ok(self) -> bool:
        return self.exception is None


def _resolve_tap_class(tap_ref: Union[str, Type[Tap]]) -> Type[Tap]:
    if isinstance(tap_ref, type) and issubclass(tap_ref, Tap):
        return tap_ref
    if isinstance(tap_ref, str):
        # Accept "package.module:Class" or "package.module.Class"
        module_name: str
        class_name: str
        if ":" in tap_ref:
            module_name, class_name = tap_ref.split(":", 1)
        else:
            parts = tap_ref.split(".")
            if len(parts) < 2:
                raise ValueError(
                    "tap_ref must be a Tap subclass, 'module:Class', or 'module.Class'"
                )
            module_name = ".".join(parts[:-1])
            class_name = parts[-1]
        mod = importlib.import_module(module_name)
        cls = getattr(mod, class_name)
        if not isinstance(cls, type) or not issubclass(cls, Tap):
            raise TypeError(f"{tap_ref!r} does not resolve to a singer_sdk.Tap subclass")
        return cls
    raise TypeError("tap_ref must be a Tap subclass or a string import path")


def run_singer_tap(
        tap_ref: Union[str, Type[Tap]],
        *,
        # Provide config as a dict or a path-like string (aligned with TapBase constructor)
        config: Optional[Union[Dict[str, Any], str]] = None,
        # Provide state as a dict or a path-like string (aligned with TapBase constructor)
        state: Optional[Union[Dict[str, Any], str]] = None,
        # Provide catalog as an object or a path-like string (aligned with TapBase constructor)
        catalog: Optional[Union[Any, str]] = None,
        buffer_max_size: int = 10 * 1024 * 1024,  # 10 MiB in-memory before spilling to disk
        log_level: int = logging.INFO,
        validate_config: bool = True,
        parse_env_config: bool = True,
        raise_on_error: bool = False,
) -> SingerTapResult:
    """
    Execute a Singer SDK Tap class and capture its stdout/stderr into SpooledTemporaryFile.

    Parameters:
      - tap_ref: Tap class or 'module:Class'/'module.Class' path.
      - config / config_path: dict or path to tap config.
      - state / state_path: dict or path to singer state.
      - catalog_obj / catalog_path: Catalog object or path to a catalog JSON.
      - buffer_max_size: Max in-memory bytes before rolling over to a real temp file.
      - log_level: Logging level for singer/tap logs.
      - validate_config: Whether to validate tap config on init.
      - parse_env_config: Allow env var overrides during tap init.
      - raise_on_error: If True, re-raise exceptions after capturing output.

    Returns:
      SingerTapResult containing file-like stdout/stderr and an exception if occurred.
    """
    TapClass: type[Tap] = _resolve_tap_class(tap_ref)

    # Prepare in-memory spooled buffers
    spooled_out = tempfile.SpooledTemporaryFile(mode="w+b", max_size=buffer_max_size)
    spooled_err = tempfile.SpooledTemporaryFile(mode="w+b", max_size=buffer_max_size)

    # Configure logging to capture into spooled_err
    # We'll attach a temporary handler on singer_sdk logger hierarchy.
    logger_names = ["singer_sdk", "singer_sdk.tap_base", "singer_sdk.target_base"]
    handlers_added = []
    try:
        log_stream = io.TextIOWrapper(spooled_err, encoding="utf-8", write_through=True)
    except Exception:
        # Fallback if TextIOWrapper fails for some reason
        log_stream = None

    def _attach_handlers():
        if log_stream is None:
            return
        for name in logger_names:
            lg = logging.getLogger(name)
            lg.setLevel(log_level)
            handler = logging.StreamHandler(log_stream)
            formatter = logging.Formatter(
                fmt="%(asctime)s %(levelname)s %(name)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            handler.setFormatter(formatter)
            lg.addHandler(handler)
            handlers_added.append((lg, handler))

    def _detach_handlers():
        for lg, handler in handlers_added:
            try:
                lg.removeHandler(handler)
            except Exception:
                pass

    # Build a human-readable "command" representation for logging/debug
    cmd_repr_parts = [
        f"tap={TapClass.__module__}.{TapClass.__name__}",
        f"config={'<dict>' if isinstance(config, dict) else (config or '<none>')}",
        f"state={'<dict>' if isinstance(state, dict) else (state or '<none>')}",
        f"catalog={'<obj>' if (catalog is not None and not isinstance(catalog, str)) else (catalog or '<none>')}",
    ]
    command_repr = "run_singer_tap(" + ", ".join(cmd_repr_parts) + ")"

    exception: Optional[BaseException] = None

    # Create a text wrapper for spooled_out/stderr redirection
    out_text = io.TextIOWrapper(spooled_out, encoding="utf-8", write_through=True)
    err_text = io.TextIOWrapper(spooled_err, encoding="utf-8", write_through=True)

    try:
        _attach_handlers()

        with redirect_stdout(out_text), redirect_stderr(err_text):
            tap = TapClass(
                config=config,
                parse_env_config=parse_env_config,
                validate_config=validate_config,
                state=state,
                catalog=catalog
            )

            tap.sync_all()

    except BaseException as exc:
        exception = exc
        if raise_on_error:
            # Rewind for consumers before re-raising
            try:
                spooled_out.seek(0)
                spooled_err.seek(0)
            except Exception:
                pass
            raise
    finally:
        _detach_handlers()
        # Ensure buffers are positioned for reading
        try:
            spooled_out.flush()
            spooled_err.flush()
            spooled_out.seek(0)
            spooled_err.seek(0)
        except Exception:
            pass

        # Ensure wrappers don't close underlying buffers on GC
        # Detach wrappers from the underlying spooled files so that
        # garbage collection of the wrappers won't close the spooled files.
        try:
            out_text.flush()
            err_text.flush()
            out_text.detach()
            err_text.detach()
            if log_stream is not None:
                log_stream.flush()
                log_stream.detach()
        except Exception:
            pass

    return SingerTapResult(
        stdout_file=spooled_out,
        stderr_file=spooled_err,
        command_repr=command_repr,
        exception=exception,
    )


@task(name="run_singer_tap_task", description="Execute run_singer_tap as an async Prefect task")
async def run_singer_tap_task(*args, **kwargs) -> SingerTapResult:
    """
    Async Prefect task wrapper around run_singer_tap.
    Executes the synchronous run_singer_tap in a thread to avoid blocking the event loop.

    Usage within a Prefect flow:
      result = await run_singer_tap_task.submit(tap_ref=..., config=..., ...)
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: run_singer_tap(*args, **kwargs))
