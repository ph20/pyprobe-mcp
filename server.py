"""pyprobe-mcp — a tiny MCP server that runs Python inside its own process.

Two tools execute code in a shared, persistent in-process namespace and return
the value of the last expression together with captured stdout/stderr:

- ``run_python``      — async; supports top-level ``await`` and a soft timeout.
- ``run_python_sync`` — plain synchronous ``exec`` (FastMCP runs it in a worker
  thread, so blocking code does not freeze the server). Simpler model, no await.

Both share the same namespace, so you can set a value with one and read it with
the other. It is a probe for inspecting a runtime from the inside — Prefect
Horizon, a Docker container, a plain host — and is meant to be deployed
temporarily and removed once the investigation is done.

Run it:

    python server.py              # stdio, for a local MCP client
    fastmcp run server.py:mcp     # HTTP, anywhere
    # Prefect Horizon entrypoint:  server.py:mcp

WARNING: these tools execute arbitrary code in this process. Keep the server
behind an authenticated deployment (on Horizon the gateway authenticates
callers) and delete it when you are finished.
"""

from __future__ import annotations

import ast
import asyncio
import contextlib
import inspect
import io
import sys
import time
import traceback
from typing import Any

from fastmcp import FastMCP

MAX_OUTPUT_CHARS = 256_000
DEFAULT_TIMEOUT = 30.0

mcp: FastMCP[Any] = FastMCP(
    name="pyprobe",
    instructions=(
        "Execute Python inside this server's own process and read back the last "
        "expression's value plus stdout/stderr. run_python is async (top-level await "
        "works); run_python_sync runs plain synchronous code in a worker thread. Both "
        "share one persistent namespace (a live REPL). Use them to inspect the runtime: "
        "environment, sys.path, loaded modules, the request headers a gateway forwards, "
        "module-level singletons, and so on."
    ),
)

# Namespace shared across all calls (async and sync) within one live process.
# Best-effort REPL state only: a platform may route a later call to a different
# instance, and concurrent calls are not synchronized (this is a single-user probe).
_NS: dict[str, Any] = {}


def _fresh_namespace() -> dict[str, Any]:
    return {"__name__": "__pyprobe__", "__builtins__": __builtins__}


def _reset_namespace(reset: bool) -> None:
    if reset or not _NS:
        _NS.clear()
        _NS.update(_fresh_namespace())


def _compile(source: str, *, allow_await: bool) -> tuple[Any, Any]:
    """Compile ``source``, splitting a trailing expression off so its value is returned.

    With ``allow_await`` the parts are compiled with ``PyCF_ALLOW_TOP_LEVEL_AWAIT``
    so ``await`` may be used at the top level (such a part yields a coroutine when
    evaluated). Without it, a top-level ``await`` is a normal SyntaxError.
    """
    tree = ast.parse(source, "<pyprobe>", "exec")
    flags = ast.PyCF_ALLOW_TOP_LEVEL_AWAIT if allow_await else 0
    if tree.body and isinstance(tree.body[-1], ast.Expr):
        head = ast.Module(body=tree.body[:-1], type_ignores=[])
        tail = ast.Expression(body=tree.body[-1].value)
        return (
            compile(head, "<pyprobe>", "exec", flags=flags),
            compile(tail, "<pyprobe>", "eval", flags=flags),
        )
    return compile(tree, "<pyprobe>", "exec", flags=flags), None


async def _execute_async(head: Any, tail: Any, ns: dict[str, Any]) -> Any:
    """Run the statements, then evaluate the trailing expression, awaiting coroutines."""
    result = eval(head, ns)  # noqa: S307 - executing the caller's code is the point
    if inspect.iscoroutine(result):
        await result
    if tail is None:
        return None
    value = eval(tail, ns)  # noqa: S307
    return await value if inspect.iscoroutine(value) else value


def _execute_sync(head: Any, tail: Any, ns: dict[str, Any]) -> Any:
    """Run the statements, then evaluate the trailing expression (no await)."""
    exec(head, ns)  # noqa: S102 - executing the caller's code is the point
    if tail is None:
        return None
    return eval(tail, ns)  # noqa: S307


def _truncate(text: str) -> tuple[str, bool]:
    if len(text) <= MAX_OUTPUT_CHARS:
        return text, False
    return text[:MAX_OUTPUT_CHARS] + "\n...[truncated]", True


def _result(
    out: io.StringIO, err: io.StringIO, value: Any, exc_text: str | None, started: float
) -> dict[str, Any]:
    value_repr: str | None = None
    if exc_text is None and value is not None:
        try:
            value_repr = repr(value)
        except Exception as exc:  # noqa: BLE001 - a broken __repr__ must not fail the call
            value_repr = f"<unrepresentable: {exc!r}>"
        value_repr, _ = _truncate(value_repr)
    stdout_text, out_trunc = _truncate(out.getvalue())
    stderr_text, err_trunc = _truncate(err.getvalue())
    return {
        "ok": exc_text is None,
        "value_repr": value_repr,
        "stdout": stdout_text,
        "stderr": stderr_text,
        "exception": exc_text,
        "duration_ms": round((time.perf_counter() - started) * 1000, 2),
        "truncated": out_trunc or err_trunc,
    }


@mcp.tool
async def run_python(
    code: str,
    stdin: str = "",
    reset: bool = False,
    timeout_seconds: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """Execute Python asynchronously in this server's own process and return the result.

    The value of the final expression (if the last line is one) is returned as
    ``value_repr``. Top-level ``await`` is supported. The namespace persists between
    calls and is shared with run_python_sync.

    Args:
        code: Python source.
        stdin: Text fed to ``sys.stdin`` for the duration of the call.
        reset: Clear the shared namespace before running.
        timeout_seconds: Soft limit; a purely CPU-bound synchronous call cannot be
            interrupted and is bounded only by the platform's own request timeout.

    Returns:
        A dict with ``ok``, ``value_repr``, ``stdout``, ``stderr``, ``exception``
        (a traceback string or null), ``duration_ms`` and ``truncated``.
    """
    _reset_namespace(reset)
    out, err = io.StringIO(), io.StringIO()
    real_stdin = sys.stdin
    value: Any = None
    exc_text: str | None = None
    started = time.perf_counter()

    sys.stdin = io.StringIO(stdin)
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            head, tail = _compile(code, allow_await=True)
            value = await asyncio.wait_for(_execute_async(head, tail, _NS), timeout_seconds)
    except asyncio.CancelledError:
        raise
    except asyncio.TimeoutError:
        exc_text = f"TimeoutError: execution exceeded {timeout_seconds}s"
    except BaseException:  # noqa: BLE001 - report any failure in the caller's code
        exc_text = "".join(traceback.format_exc())
    finally:
        sys.stdin = real_stdin

    return _result(out, err, value, exc_text, started)


@mcp.tool
def run_python_sync(
    code: str,
    stdin: str = "",
    reset: bool = False,
) -> dict[str, Any]:
    """Execute Python synchronously in this server's own process and return the result.

    A plain ``exec`` with no async machinery; FastMCP runs it in a worker thread, so
    blocking code (sleeps, blocking I/O, heavy CPU) does not freeze the server. There
    is no ``await`` and no timeout — the call runs to completion. The namespace
    persists between calls and is shared with run_python.

    Args:
        code: Python source (top-level ``await`` is not allowed here).
        stdin: Text fed to ``sys.stdin`` for the duration of the call.
        reset: Clear the shared namespace before running.

    Returns:
        The same dict shape as run_python.
    """
    _reset_namespace(reset)
    out, err = io.StringIO(), io.StringIO()
    real_stdin = sys.stdin
    value: Any = None
    exc_text: str | None = None
    started = time.perf_counter()

    sys.stdin = io.StringIO(stdin)
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            head, tail = _compile(code, allow_await=False)
            value = _execute_sync(head, tail, _NS)
    except BaseException:  # noqa: BLE001 - report any failure in the caller's code
        exc_text = "".join(traceback.format_exc())
    finally:
        sys.stdin = real_stdin

    return _result(out, err, value, exc_text, started)


if __name__ == "__main__":
    mcp.run(show_banner=False)
