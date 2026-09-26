# pyprobe-mcp

A tiny, **temporary** MCP server for inspecting a runtime from the inside. It
executes Python **in the server's own process** and returns the value of the last
expression plus captured stdout/stderr. Use it to look around a deployment —
Prefect Horizon, a Docker container, a plain host — then delete it.

> ⚠️ These tools run arbitrary code in this process. Deploy the server only where
> the caller is already authenticated (on Prefect Horizon the gateway
> authenticates callers) and **remove it once you are done investigating.**

## The tools

Two tools, one **shared persistent namespace** (set a value with one, read it with
the other — a live REPL across calls, so you can inspect module-level singletons,
settings, open clients and the environment over several calls):

`run_python(code, stdin="", reset=False, timeout_seconds=30.0)` — asynchronous.
**Top-level `await` is supported** and a soft `timeout_seconds` applies.

`run_python_sync(code, stdin="", reset=False)` — plain synchronous `exec`, simpler
model, no `await`. FastMCP runs it in a worker thread, so blocking code (sleeps,
blocking I/O, heavy CPU) does not freeze the server; there is no timeout — it runs
to completion.

Common behaviour:

- If the last line is an expression, its value comes back as `value_repr` (`repr`).
- `stdin` is fed to `sys.stdin` for the duration of the call.
- `reset=True` clears the shared namespace first.
- Both return a dict: `ok`, `value_repr`, `stdout`, `stderr`, `exception` (a
  traceback string or `null`), `duration_ms`, `truncated`. Output is truncated at
  ~256 KB (the platform payload ceiling on Horizon is 6 MB).

This is a single-user probe: concurrent calls are not synchronized (`sys.stdout`
and `sys.stdin` are swapped process-wide during a call), which is fine for
interactive investigation.

## Run it

```bash
uv sync

python server.py               # stdio, for a local MCP client
uv run fastmcp run server.py:mcp   # HTTP on 127.0.0.1:8000/mcp
```

Prefect Horizon entrypoint: **`server.py:mcp`** (Horizon runs the module-level
`mcp` object itself; it forwards only `POST /mcp`).

## Example

```python
import sys, platform, os
print("python", platform.python_version())
{"argv": sys.argv, "cwd": os.getcwd(), "env_keys": sorted(os.environ)[:10]}
```
