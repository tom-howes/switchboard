"""Entrypoint: `python -m switchboard serve` or `python -m switchboard worker`.

Why this file exists rather than just running `uvicorn switchboard.main:app`:
on Windows, asyncio defaults to the ProactorEventLoop, and psycopg's async mode
cannot run on it (it needs the selector-based loop that every other OS uses by
default). Running uvicorn from the command line gives us no clean way to choose
the loop, so we start the server ourselves inside a loop we picked. On Linux
and macOS the selector loop is already the default, so this changes nothing
there.
"""

from __future__ import annotations

import asyncio
import selectors
import sys

import uvicorn

from .config import get_settings


def _loop_factory() -> asyncio.AbstractEventLoop:
    return asyncio.SelectorEventLoop(selectors.SelectSelector())


def run(coro) -> None:
    if sys.platform == "win32":
        asyncio.run(coro, loop_factory=_loop_factory)
    else:
        asyncio.run(coro)


def serve() -> None:
    # loop="none" tells uvicorn not to install its own loop: we already did.
    config = uvicorn.Config("switchboard.main:app", port=get_settings().port, loop="none")
    run(uvicorn.Server(config).serve())


def worker() -> None:
    from .engine.worker import run_forever

    run(run_forever())


COMMANDS = {"serve": serve, "worker": worker}


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "serve"
    if name not in COMMANDS:
        raise SystemExit(f"usage: python -m switchboard [{'|'.join(COMMANDS)}]")
    COMMANDS[name]()
