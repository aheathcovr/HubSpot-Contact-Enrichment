"""
setup_alerting.sh: the log counters and alert policies that tell someone when the service is failing.

Nothing alerted on this service until this script existed, and a broken alert is silent by nature, so these tests run the
real script against a fake gcloud and pin what must hold: a dry run touches nothing, every policy reads a counter the
script creates, counters exist before the policies that read them (Monitoring rejects a policy whose counter is not
there yet), re-running creates nothing twice, and the filter for refused HubSpot-app requests keeps the clauses that make
it useful (a bad signature or a missing secret alerts, someone probing the public address without a signature does not).

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_setup_alerting.py" -v
"""

import json
import os
import re
import shlex
import stat
import subprocess
import textwrap

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPT = os.path.join(_HERE, "..", "setup_alerting.sh")

_CARD_ALERT = "hubspot_app_requests_refused"


def _fake(path, body):
    with open(path, "w") as f:
        f.write("#!/bin/bash\n" + textwrap.dedent(body))
    os.chmod(path, os.stat(path).st_mode | stat.S_IEXEC)


@pytest.fixture
def box(tmp_path):
    """A fake gcloud that logs every call and answers as a project that has (all) or has not (none) been set up."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _fake(bin_dir / "gcloud", """
        echo "$*" >> "$FAKE_LOG"
        case "$*" in
          "logging metrics describe"*) [ "$FAKE_EXISTING" = "all" ] && exit 0 || exit 1 ;;
          "beta monitoring channels list"*) [ "$FAKE_EXISTING" = "all" ] && echo "projects/p/notificationChannels/111"; exit 0 ;;
          "beta monitoring channels create"*) echo "projects/p/notificationChannels/222"; exit 0 ;;
          "beta monitoring policies list"*) [ "$FAKE_EXISTING" = "all" ] && echo "projects/p/alertPolicies/1"; exit 0 ;;
          "beta monitoring policies create"*)
            [ -n "$FAKE_POLICY_FAILS" ] && { echo "ERROR: counter not found" >&2; exit 1; }
            for a in "$@"; do case "$a" in --policy-from-file=*) cat "${a#--policy-from-file=}" >> "$FAKE_LOG.policies" ;; esac; done
            exit 0 ;;
          *) exit 0 ;;
        esac
    """)
    return tmp_path, bin_dir


def _run(box, *, apply=None, existing="none", email="alerts@example.test", extra_env=None):
    """*apply* is True, False, or None to leave APPLY unset, as when someone just runs the script."""
    tmp_path, bin_dir = box
    env = {"PATH": f"{bin_dir}:{os.environ['PATH']}", "HOME": str(tmp_path), "FAKE_LOG": str(tmp_path / "log"),
           "FAKE_EXISTING": existing}
    if apply is not None:
        env["APPLY"] = "1" if apply else "0"
    if email is not None:
        env["NOTIFY_EMAIL"] = email
    env.update(extra_env or {})
    done = subprocess.run(["bash", _SCRIPT], capture_output=True, text=True, env=env, timeout=60)
    log = (tmp_path / "log").read_text().splitlines() if (tmp_path / "log").exists() else []
    policies = (tmp_path / "log.policies").read_text() if (tmp_path / "log.policies").exists() else ""
    return done, log, policies


def _dry_run_commands(box):
    """The gcloud commands a dry run prints, each split into words as a shell would."""
    done, log, _ = _run(box, apply=False)
    assert done.returncode == 0, done.stderr
    return done.stdout, [shlex.split(line.strip()[2:]) for line in done.stdout.splitlines() if line.startswith("  + ")]


def _metrics_created(commands):
    return {c[4]: dict(arg.split("=", 1) for arg in c if arg.startswith("--") and "=" in arg)
            for c in commands if c[:4] == ["gcloud", "logging", "metrics", "create"]}


def _policies_printed(stdout):
    """Each policy's JSON as a dry run prints it: after its '--- name' line, indented four spaces, up to the create command."""
    policies, block = [], None
    for line in stdout.splitlines():
        if line.startswith("--- HubSpot"):
            block = []
        elif block is not None and line.startswith("  + gcloud beta monitoring policies create"):
            policies.append(json.loads("\n".join(block)))
            block = None
        elif block is not None and line.startswith("    "):
            block.append(line[4:])
    return policies


# ── A dry run changes nothing ────────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("apply", [None, False])
def test_a_dry_run_makes_no_gcloud_call_at_all(box, apply):
    done, log, _ = _run(box, apply=apply)
    assert done.returncode == 0
    assert "DRY RUN" in done.stdout
    assert log == []


def test_applying_without_a_recipient_stops_before_anything_is_created(box):
    done, log, _ = _run(box, apply=True, email=None)
    assert done.returncode == 1
    assert "NOTIFY_EMAIL is required" in done.stderr
    assert log == []


def test_no_email_address_is_written_into_the_script():
    with open(_SCRIPT) as f:
        addresses = set(re.findall(r"[\w.+-]+@[\w-]+\.[\w.-]+", f.read()))
    assert addresses <= {"you@example.com"}


# ── What it sets up ──────────────────────────────────────────────────────────────────────────────────────────────────

def test_every_counter_is_limited_to_this_service(box):
    _, commands = _dry_run_commands(box)
    metrics = _metrics_created(commands)
    assert len(metrics) == 7
    for name, options in metrics.items():
        assert 'resource.labels.service_name="hubspot-enrichment"' in options["--log-filter"], name


def test_every_policy_reads_a_counter_the_script_creates(box):
    stdout, commands = _dry_run_commands(box)
    created = set(_metrics_created(commands))
    policies = _policies_printed(stdout)
    assert len(policies) == 6
    for policy in policies:
        for condition in policy["conditions"]:
            read = re.search(r"logging\.googleapis\.com/user/(\w+)", condition["conditionThreshold"]["filter"]).group(1)
            assert read in created, f'{policy["displayName"]} reads {read}, which nothing creates'


def test_every_counter_is_read_by_some_policy(box):
    stdout, commands = _dry_run_commands(box)
    read = {re.search(r"user/(\w+)", c["conditionThreshold"]["filter"]).group(1)
            for p in _policies_printed(stdout) for c in p["conditions"]}
    assert set(_metrics_created(commands)) <= read


def test_every_policy_is_enabled_names_a_channel_and_says_what_to_do(box):
    stdout, _ = _dry_run_commands(box)
    for policy in _policies_printed(stdout):
        assert policy["enabled"] is True
        assert policy["notificationChannels"]
        assert len(policy["documentation"]["content"]) > 100, policy["displayName"]


# ── Refused requests from the HubSpot app ────────────────────────────────────────────────────────────────────────────

def _card_filter(box):
    _, commands = _dry_run_commands(box)
    return _metrics_created(commands)[_CARD_ALERT]["--log-filter"]


def test_a_request_with_no_signature_is_not_counted_but_a_wrong_one_is(box):
    f = _card_filter(box)
    assert 'jsonPayload.message:"/app request refused"' in f
    assert 'NOT jsonPayload.message:"Missing signature"' in f


def test_a_service_with_no_secret_is_caught_from_the_request_log(box):
    # The service answers 503 for it and does not log it, so only the request log can show it.
    assert 'httpRequest.requestUrl:"/app/" AND httpRequest.status=503' in _card_filter(box)


def test_a_rejected_cloud_tasks_callback_for_a_run_is_counted(box):
    f = _card_filter(box)
    assert 'httpRequest.requestUrl:"/app/run"' in f and "httpRequest.status=401" in f
    assert 'httpRequest.userAgent:"Google-Cloud-Tasks"' in f


def test_the_card_alert_fires_on_a_single_refusal(box):
    stdout, _ = _dry_run_commands(box)
    policy = next(p for p in _policies_printed(stdout) if p["displayName"] == "HubSpot app: card requests refused")
    condition = policy["conditions"][0]["conditionThreshold"]
    assert _CARD_ALERT in condition["filter"]
    assert condition["thresholdValue"] == 0 and condition["comparison"] == "COMPARISON_GT"


# ── Applying ─────────────────────────────────────────────────────────────────────────────────────────────────────────

def test_a_first_apply_creates_the_counters_then_the_channel_then_the_policies(box):
    done, log, policies = _run(box, apply=True, existing="none")
    assert done.returncode == 0, done.stderr
    kinds = ["metric" if "metrics create" in line else "channel" if "channels create" in line
             else "policy" if "policies create" in line else None for line in log]
    kinds = [k for k in kinds if k]
    assert kinds.count("metric") == 7 and kinds.count("channel") == 1 and kinds.count("policy") == 6
    # Monitoring rejects a policy whose counter does not exist yet, so every counter comes first.
    assert max(i for i, k in enumerate(kinds) if k == "metric") < min(i for i, k in enumerate(kinds) if k == "policy")
    assert kinds.index("channel") < kinds.index("policy")


def test_the_policies_are_pointed_at_the_channel_just_created(box):
    _, _, policies = _run(box, apply=True, existing="none")
    assert policies.count("projects/p/notificationChannels/222") == 6


def test_the_channel_is_an_email_to_the_given_recipient(box):
    _, log, _ = _run(box, apply=True, existing="none", email="team@example.test")
    create = next(line for line in log if "channels create" in line)
    assert "--type=email" in create and "email_address=team@example.test" in create


def test_running_it_again_creates_nothing_twice(box):
    done, log, policies = _run(box, apply=True, existing="all")
    assert done.returncode == 0, done.stderr
    assert not any(w in line for line in log for w in ("metrics create", "channels create", "policies create"))
    assert sum("metrics update" in line for line in log) == 7
    assert policies == ""


def test_a_failure_part_way_stops_the_script_with_an_error(box):
    done, _, _ = _run(box, apply=True, existing="none", extra_env={"FAKE_POLICY_FAILS": "1"})
    assert done.returncode != 0
    assert "Done." not in done.stdout
