"""
setup_app_secret.sh: how the secret the app's function signs with, and the service checks, gets created.

The secret is a random string that must end up in two places, Secret Manager (for the service) and HubSpot (for the app's
function), and must never be printed, logged or kept in a file. HubSpot's CLI only takes a secret at a hidden prompt, so
the script puts it on the clipboard for a paste. These tests run the real script with fake gcloud and clipboard commands
and pin what matters: a dry run changes nothing and makes no secret, the value never appears in the script's output, an
existing secret is never replaced, an error reading the project stops it before anything is created, and the clipboard
is cleared afterwards.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_setup_app_secret.py" -v
"""

import os
import re
import stat
import subprocess
import textwrap
import time

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPT = os.path.join(_HERE, "..", "setup_app_secret.sh")
_EXISTING_VALUE = "existing-secret-value-0123456789abcdef"


def _fake(path, body):
    with open(path, "w") as f:
        f.write("#!/bin/bash\n" + textwrap.dedent(body))
    os.chmod(path, os.stat(path).st_mode | stat.S_IEXEC)


@pytest.fixture
def box(tmp_path):
    """A directory of fake commands that log what they were asked and take the place of the real ones."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    # gcloud: state comes from FAKE_STATE (missing, exists or broken); every call is logged; a create's stdin is kept.
    _fake(bin_dir / "gcloud", """
        echo "$*" >> "$FAKE_LOG"
        case "$*" in
          "secrets list"*)
            [ "$FAKE_STATE" = "broken" ] && { echo "ERROR: Reauthentication failed" >&2; exit 1; }
            [ "$FAKE_STATE" = "exists" ] && echo "projects/1/secrets/APP_REQUEST_SECRET"
            exit 0 ;;
          "secrets create"*) cat > "$FAKE_LOG.created"; exit 0 ;;
          "secrets versions access"*) printf '%s' "$FAKE_EXISTING"; exit 0 ;;
          *) exit 0 ;;
        esac
    """)
    # pbcopy: keeps what it was given, as the clipboard.
    _fake(bin_dir / "pbcopy", 'cat > "$FAKE_CLIP"; echo "clipboard set" >> "$FAKE_LOG"\n')
    return tmp_path, bin_dir


def _env(box, state, apply, extra_env=None, confirm_from=None):
    """*apply* is True, False, or None to leave APPLY unset, as when someone just runs the script."""
    tmp_path, bin_dir = box
    enter = tmp_path / "enter"
    enter.write_text("\n")
    env = {
        "PATH": f"{bin_dir}:{os.environ['PATH']}", "HOME": str(tmp_path), "FAKE_STATE": state, "FAKE_LOG": str(tmp_path / "log"),
        "FAKE_CLIP": str(tmp_path / "clip"), "FAKE_EXISTING": _EXISTING_VALUE,
        # Where the script reads "press Enter" from: a terminal in real use, a file that already says Enter here.
        "CONFIRM_FROM": str(confirm_from or enter),
    }
    if apply is not None:
        env["APPLY"] = "1" if apply else "0"
    env.update(extra_env or {})
    return env


def _run(box, *, state="missing", apply=False, extra_env=None):
    tmp_path, bin_dir = box
    env = _env(box, state, apply, extra_env)
    done = subprocess.run(["bash", _SCRIPT], capture_output=True, text=True, env=env, timeout=60)
    log = (tmp_path / "log").read_text() if (tmp_path / "log").exists() else ""
    created = (tmp_path / "log.created").read_text() if (tmp_path / "log.created").exists() else None
    clip = (tmp_path / "clip").read_text() if (tmp_path / "clip").exists() else None
    return done, log, created, clip


def _calls(log):
    return [line for line in log.splitlines() if line]


# ── A dry run ─────────────────────────────────────────────────────────────────

def test_a_dry_run_only_looks_and_says_what_it_would_do(box):
    done, log, created, clip = _run(box, state="missing", apply=False)

    assert done.returncode == 0 and "DRY RUN" in done.stdout
    assert [c for c in _calls(log) if not c.startswith("secrets list")] == []
    assert created is None and clip is None
    assert "gcloud secrets create APP_REQUEST_SECRET" in done.stdout


def test_running_it_with_nothing_set_is_a_dry_run(box):
    done, log, created, clip = _run(box, state="missing", apply=None)

    assert "DRY RUN" in done.stdout and created is None and clip is None
    assert [c for c in _calls(log) if not c.startswith("secrets list")] == []


# ── Creating the secret ───────────────────────────────────────────────────────

def test_it_creates_the_secret_from_a_random_value_and_lets_the_service_read_it(box):
    done, log, created, clip = _run(box, state="missing", apply=True)

    assert done.returncode == 0
    assert created is not None and len(created) >= 32
    assert any(c.startswith("secrets create APP_REQUEST_SECRET") and "--data-file=-" in c for c in _calls(log))
    grant = [c for c in _calls(log) if c.startswith("secrets add-iam-policy-binding APP_REQUEST_SECRET")]
    assert len(grant) == 1 and "roles/secretmanager.secretAccessor" in grant[0] and "hubspot-enrichment-runtime@" in grant[0]


def test_it_makes_a_different_value_each_time(box):
    first = _run(box, state="missing", apply=True)[2]
    second = _run(box, state="missing", apply=True)[2]

    assert first != second


def test_the_value_is_never_printed_or_put_on_a_command_line(box):
    done, log, created, clip = _run(box, state="missing", apply=True)

    assert created and created not in done.stdout and created not in done.stderr and created not in log
    assert not re.search(re.escape(created[:12]), done.stdout + done.stderr + log)


def test_the_same_value_goes_on_the_clipboard_for_hubspots_hidden_prompt_and_is_cleared_after(box):
    done, log, created, clip = _run(box, state="missing", apply=True)

    # The clipboard was set to the value, then set empty: the last thing it holds is nothing.
    assert _calls(log).count("clipboard set") == 2
    assert clip == ""
    assert "hs secret add APP_REQUEST_SECRET" in done.stdout


def test_it_names_the_hubspot_account_to_add_the_secret_to(box):
    done, *_ = _run(box, state="missing", apply=True, extra_env={"HUBSPOT_ACCOUNT": "52218377"})

    assert "hs secret add APP_REQUEST_SECRET --account 52218377" in done.stdout


# ── A secret that is already there ────────────────────────────────────────────

def test_it_never_replaces_a_secret_that_exists_and_never_touches_the_clipboard(box):
    done, log, created, clip = _run(box, state="exists", apply=True)

    assert done.returncode == 0 and "already exists" in done.stdout.lower()
    assert created is None and clip is None
    assert not any(c.startswith(("secrets create", "secrets versions add", "secrets delete")) for c in _calls(log))


def test_it_can_copy_the_existing_value_for_another_hubspot_account_without_printing_it(box):
    done, log, created, clip = _run(box, state="exists", apply=True, extra_env={"COPY_EXISTING": "1"})

    assert done.returncode == 0 and created is None
    assert _EXISTING_VALUE not in done.stdout + done.stderr + log
    assert any(c.startswith("secrets versions access latest --secret=APP_REQUEST_SECRET") for c in _calls(log))
    assert _calls(log).count("clipboard set") == 2 and clip == ""


# ── When it cannot tell ───────────────────────────────────────────────────────

def test_an_error_reading_the_project_stops_it_before_anything_is_created(box):
    done, log, created, clip = _run(box, state="broken", apply=True)

    assert done.returncode != 0
    assert created is None and clip is None
    assert not any(c.startswith(("secrets create", "secrets add-iam-policy-binding")) for c in _calls(log))


# ── It waits for the paste ────────────────────────────────────────────────────

def test_it_keeps_the_value_on_the_clipboard_until_the_rep_has_pasted_it_and_says_so(box):
    tmp_path, _ = box
    fifo = tmp_path / "enter-later"
    os.mkfifo(fifo)
    proc = subprocess.Popen(["bash", _SCRIPT], env=_env(box, "missing", True, confirm_from=fifo), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        # Wait until the value is on the clipboard.
        for _ in range(100):
            if (tmp_path / "log").exists() and "clipboard set" in (tmp_path / "log").read_text():
                break
            time.sleep(0.05)
        time.sleep(0.5)

        assert proc.poll() is None, "the script went on without waiting for Enter"
        assert (tmp_path / "clip").read_text() == (tmp_path / "log.created").read_text() != ""

        with open(fifo, "w") as enter:
            enter.write("\n")
        proc.wait(timeout=20)

        assert proc.returncode == 0 and (tmp_path / "clip").read_text() == ""
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.communicate()
