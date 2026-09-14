"""
ComfyUI Dead Man's Queue - power-loss-safe persistence for the prompt queue.

ComfyUI keeps its queue entirely in RAM (``execution.PromptQueue.queue`` and
``.currently_running``), so an unexpected power cut loses every job that was
waiting. This extension mirrors the queue into a crash-safe SQLite database and
pushes the survivors back in on the next start.

Design notes
------------
* Durability: SQLite in WAL mode with ``synchronous=FULL``, so every commit is
  fsync'd before it is acknowledged. A plain JSON dump can be truncated to zero
  bytes by a power cut landing mid-write, which would lose the entire queue -
  the exact failure this is insuring against.
* Secrets: a queue item's 6th field holds ``auth_token_comfy_org`` /
  ``api_key_comfy_org`` (``execution.SENSITIVE_EXTRA_DATA_KEYS``). It is
  stripped before anything reaches disk, mirroring the ``remove_sensitive``
  that core itself applies before writing to history.
* Fallback format: on restore, each recovered job's full editor graph is also
  written out as a normal workflow ``.json``. That stays openable even if a
  ComfyUI or custom-node update ever makes the stored prompt unreplayable.
* No frontend changes: the native queue UI keeps working as-is, so this does
  not fight other extensions that manipulate the queue object.
* Install point: custom nodes load after the queue object exists but before the
  worker thread starts, so wrapping it here is race-free.

Set ``COMFY_DEADMANSQUEUE_DISABLE=1`` to turn persistence off.
"""

import json
import logging
import os
import sqlite3
import threading
import time

from aiohttp import web

import folder_paths
from server import PromptServer

DB_FILENAME = "deadmansqueue.sqlite3"
ENV_DISABLE = "COMFY_DEADMANSQUEUE_DISABLE"
LIVE_STATES = ("pending", "running")

_log = logging.getLogger(__name__)
_db_lock = threading.Lock()
_conn = None
_enabled = False
_restored_once = False
_last_restore = None


def _say(msg, *args):
    logging.info("[DeadMansQueue] " + msg, *args)


# --------------------------------------------------------------------------
# storage
# --------------------------------------------------------------------------

def _db_path():
    user_dir = folder_paths.get_user_directory()
    os.makedirs(user_dir, exist_ok=True)
    return os.path.join(user_dir, DB_FILENAME)


def _open_db():
    conn = sqlite3.connect(_db_path(), check_same_thread=False, timeout=30.0)
    # WAL keeps the database readable after a torn write; FULL fsyncs the log on
    # every commit. Together they are what make this survive a power cut rather
    # than only a clean process crash.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS jobs (
               prompt_id  TEXT    PRIMARY KEY,
               number     REAL    NOT NULL,
               state      TEXT    NOT NULL,
               tuple_len  INTEGER NOT NULL,
               item_json  TEXT    NOT NULL,
               queued_at  REAL    NOT NULL
           )"""
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs (state)")
    conn.commit()
    return conn


def _strip_sensitive(item):
    """Drop the credential field, matching core's own ``remove_sensitive``
    (``prompt[:5] + prompt[6:]``). The original length is kept so the exact
    tuple shape can be rebuilt, which keeps this forward-compatible if ComfyUI
    appends further fields."""
    shape = tuple(item)
    if len(shape) > 5:
        return list(shape[:5] + shape[6:]), len(shape)
    return list(shape), len(shape)


def _restore_sensitive(stored, tuple_len):
    """Reinsert an empty credential dict at index 5 so the worker's ``item[5]``
    lookup keeps working. API-node jobs will need re-authenticating, which is
    the correct trade for never writing tokens to disk."""
    shape = tuple(stored)
    if tuple_len > 5:
        return shape[:5] + ({},) + shape[5:]
    return shape


def _record(item, state="pending"):
    stored, tuple_len = _strip_sensitive(item)
    with _db_lock:
        _conn.execute(
            "INSERT OR REPLACE INTO jobs"
            " (prompt_id, number, state, tuple_len, item_json, queued_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                str(item[1]),
                float(item[0]),
                state,
                tuple_len,
                json.dumps(stored, separators=(",", ":")),
                time.time(),
            ),
        )
        _conn.commit()


def _set_state(prompt_id, state):
    with _db_lock:
        _conn.execute(
            "UPDATE jobs SET state = ? WHERE prompt_id = ?", (state, str(prompt_id))
        )
        _conn.commit()


def _forget(prompt_id):
    with _db_lock:
        _conn.execute("DELETE FROM jobs WHERE prompt_id = ?", (str(prompt_id),))
        _conn.commit()


def _resync_pending(queue):
    """Drop rows for jobs that are no longer queued.

    Used after the bulk removals (wipe / delete-by-predicate), where
    reconstructing which items went away is fragile - reading the live queue
    back is always the truth, and the queue is only ever tens of items long.
    """
    _running, pending = queue.get_current_queue_volatile()
    live = {str(entry[1]) for entry in pending}
    with _db_lock:
        rows = _conn.execute(
            "SELECT prompt_id FROM jobs WHERE state = 'pending'"
        ).fetchall()
        stale = [(row[0],) for row in rows if row[0] not in live]
        if stale:
            _conn.executemany("DELETE FROM jobs WHERE prompt_id = ?", stale)
            _conn.commit()
    return len(stale)


# --------------------------------------------------------------------------
# recovery
# --------------------------------------------------------------------------

def _dump_graphs(rows):
    """Write each recovered job's UI graph out as an openable workflow file.

    A queued item carries the complete editor graph in
    ``extra_data.extra_pnginfo.workflow`` - the same payload ComfyUI embeds in
    saved PNGs - not merely the flattened API prompt. Dumping it gives a
    recovery path that does not depend on the queue replay succeeding.
    """
    out_dir = os.path.join(
        folder_paths.get_user_directory(),
        "deadmansqueue",
        "recovered_" + time.strftime("%Y-%m-%d_%H-%M-%S"),
    )
    written = 0
    for seq, row in enumerate(rows, 1):
        prompt_id, _number, _state, _tuple_len, item_json, _queued_at = row
        try:
            item = json.loads(item_json)
            extra_data = item[3] if len(item) > 3 and isinstance(item[3], dict) else {}
            graph = (extra_data.get("extra_pnginfo") or {}).get("workflow")
            if not graph:
                continue
            os.makedirs(out_dir, exist_ok=True)
            filename = "%02d_%s.json" % (seq, str(prompt_id)[:8])
            with open(os.path.join(out_dir, filename), "w", encoding="utf-8") as handle:
                json.dump(graph, handle, indent=2)
            written += 1
        except Exception:
            _log.exception("[DeadMansQueue] could not dump graph for %s", prompt_id)
    return (out_dir if written else None), written


async def _on_startup(_app):
    """Re-queue everything that did not finish last session.

    Runs from aiohttp's startup signal, which fires after every custom node has
    loaded and after the worker thread is already waiting - so restored jobs
    begin immediately.
    """
    global _restored_once, _last_restore
    if not _enabled or _restored_once:
        return
    _restored_once = True

    try:
        with _db_lock:
            rows = _conn.execute(
                "SELECT prompt_id, number, state, tuple_len, item_json, queued_at"
                " FROM jobs WHERE state IN (?, ?)"
                " ORDER BY number ASC, queued_at ASC",
                LIVE_STATES,
            ).fetchall()

        if not rows:
            _say("nothing to restore - last session finished cleanly")
            return

        interrupted = sum(1 for row in rows if row[2] == "running")
        _say(
            "found %d unfinished job(s) from the last session (%d was mid-render)",
            len(rows),
            interrupted,
        )

        graph_dir, graph_count = _dump_graphs(rows)

        server = PromptServer.instance
        queue = server.prompt_queue
        restored = 0
        highest = None

        for row in rows:
            prompt_id, number, _state, tuple_len, item_json, _queued_at = row
            try:
                item = _restore_sensitive(json.loads(item_json), tuple_len)
                # Pushed straight back in without re-validating: the prompt was
                # valid when queued, and the executor already reports a normal
                # node error if something has since changed underneath it.
                queue.put(item)
                restored += 1
                highest = number if highest is None else max(highest, number)
            except Exception:
                _log.exception("[DeadMansQueue] failed to restore %s", prompt_id)
                _forget(prompt_id)

        # Keep freshly submitted prompts ordered behind the recovered ones.
        if highest is not None:
            try:
                server.number = max(server.number, int(highest) + 1)
            except Exception:
                _log.exception("[DeadMansQueue] could not advance queue counter")

        _last_restore = {
            "at": time.time(),
            "restored": restored,
            "interrupted": interrupted,
            "graphs_written": graph_count,
            "graph_dir": graph_dir,
        }
        _say("restored %d job(s) - the queue is running again", restored)
        if graph_dir:
            _say("editable copies of those graphs: %s", graph_dir)
    except Exception:
        _log.exception("[DeadMansQueue] restore failed; the queue starts empty")


# --------------------------------------------------------------------------
# queue instrumentation
# --------------------------------------------------------------------------

def _install(queue):
    original_put = queue.put
    original_get = queue.get
    original_task_done = queue.task_done
    original_wipe_queue = queue.wipe_queue
    original_delete_queue_item = queue.delete_queue_item

    def put(item):
        # Persist before enqueueing. A ghost row - a job that gets run twice -
        # is a far better failure mode than a job that vanishes.
        try:
            _record(item, "pending")
        except Exception:
            _log.exception("[DeadMansQueue] could not persist incoming job")
        return original_put(item)

    def get(timeout=None):
        result = original_get(timeout=timeout)
        if result is not None:
            try:
                _set_state(result[0][1], "running")
            except Exception:
                _log.exception("[DeadMansQueue] could not mark job as running")
        return result

    def task_done(item_id, *args, **kwargs):
        # Read the prompt id before delegating: task_done pops the entry out of
        # currently_running, so afterwards there is nothing left to look up.
        prompt_id = None
        try:
            with queue.mutex:
                running = queue.currently_running.get(item_id)
                if running is not None:
                    prompt_id = running[1]
        except Exception:
            _log.exception("[DeadMansQueue] could not identify finished job")

        result = original_task_done(item_id, *args, **kwargs)

        if prompt_id is not None:
            try:
                _forget(prompt_id)
            except Exception:
                _log.exception("[DeadMansQueue] could not clear finished job")
        return result

    def wipe_queue():
        result = original_wipe_queue()
        try:
            _resync_pending(queue)
        except Exception:
            _log.exception("[DeadMansQueue] resync after wipe failed")
        return result

    def delete_queue_item(function):
        result = original_delete_queue_item(function)
        if result:
            try:
                _resync_pending(queue)
            except Exception:
                _log.exception("[DeadMansQueue] resync after delete failed")
        return result

    queue.put = put
    queue.get = get
    queue.task_done = task_done
    queue.wipe_queue = wipe_queue
    queue.delete_queue_item = delete_queue_item


@PromptServer.instance.routes.get("/deadmansqueue/status")
async def _status(_request):
    counts = {"pending": 0, "running": 0}
    if _enabled:
        try:
            with _db_lock:
                for state, count in _conn.execute(
                    "SELECT state, COUNT(*) FROM jobs GROUP BY state"
                ):
                    counts[state] = count
        except Exception:
            _log.exception("[DeadMansQueue] status query failed")
    return web.json_response(
        {
            "enabled": _enabled,
            "database": _db_path() if _enabled else None,
            "persisted_pending": counts.get("pending", 0),
            "persisted_running": counts.get("running", 0),
            "last_restore": _last_restore,
        }
    )


def _setup():
    global _conn, _enabled

    if os.environ.get(ENV_DISABLE, "").strip().lower() not in ("", "0", "false", "no"):
        _say("disabled via %s - the queue will not be persisted", ENV_DISABLE)
        return

    server = getattr(PromptServer, "instance", None)
    if server is None or getattr(server, "prompt_queue", None) is None:
        _say("no prompt queue available - persistence disabled")
        return

    try:
        _conn = _open_db()
    except Exception:
        _log.exception("[DeadMansQueue] could not open database - persistence disabled")
        return

    _enabled = True
    _install(server.prompt_queue)
    server.app.on_startup.append(_on_startup)
    _say("armed - queue mirrored to %s", _db_path())


try:
    _setup()
except Exception:
    # Never let a persistence problem stop ComfyUI from starting.
    _log.exception("[DeadMansQueue] initialisation failed - persistence disabled")

# This extension contributes no nodes; it only instruments the queue.
NODE_CLASS_MAPPINGS = {}
NODE_DISPLAY_NAME_MAPPINGS = {}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS"]
