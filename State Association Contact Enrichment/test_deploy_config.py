"""
The deploy has to hand the service every secret the code reads.

Every push to main runs cloudbuild.yaml, whose `gcloud run deploy --set-secrets` REPLACES the
service's whole secret list with the one written there. On 2026-10-25 BounceBan and Seamless
were attached to the service by hand; the next merge's deploy reset the list to the original
five and both vendors silently went off again (every client skips quietly without its key).

These tests keep the three places that name the secrets in step: the code that reads them,
the deploy that mounts them, and the script that lets the runtime account read them.

Run with:
    python3 -m pytest "State Association Contact Enrichment/test_deploy_config.py" -v
"""

import re
from pathlib import Path

import pytest
import yaml

APP = Path(__file__).resolve().parent
ROOT = APP.parent

_ENV_READ = re.compile(
    r"""os\.(?:getenv|environ\.get)\(\s*["']([A-Z0-9_]+)["']|os\.environ\[\s*["']([A-Z0-9_]+)["']\]"""
)


def _is_secret_name(name: str) -> bool:
    return name.endswith("_API_KEY") or name.endswith("_SECRET")


def _secrets_read_by_the_code() -> set[str]:
    found: set[str] = set()
    for path in APP.rglob("*.py"):
        if path.name.startswith("test_") or path.name == "conftest.py":
            continue
        for match in _ENV_READ.finditer(path.read_text()):
            name = match.group(1) or match.group(2)
            if _is_secret_name(name):
                found.add(name)
    return found


def _secrets_mounted_by_the_deploy() -> dict[str, str]:
    """environment variable -> secret name, from every --set-secrets / --update-secrets in cloudbuild.yaml."""
    config = yaml.safe_load((ROOT / "cloudbuild.yaml").read_text())
    mounted: dict[str, str] = {}
    for step in config["steps"]:
        for arg in step.get("args", []):
            flag, _, value = str(arg).partition("=")
            if flag in ("--set-secrets", "--update-secrets"):
                for pair in value.split(","):
                    env, _, ref = pair.partition("=")
                    mounted[env.strip()] = ref.split(":")[0].strip()
    return mounted


def _env_vars_set_by_the_deploy() -> dict[str, str]:
    """environment variable -> value, from every --set-env-vars / --update-env-vars in cloudbuild.yaml."""
    config = yaml.safe_load((ROOT / "cloudbuild.yaml").read_text())
    set_by_the_deploy: dict[str, str] = {}
    for step in config["steps"]:
        for arg in step.get("args", []):
            flag, _, value = str(arg).partition("=")
            if flag in ("--set-env-vars", "--update-env-vars"):
                for pair in value.split(","):
                    env, _, setting = pair.partition("=")
                    set_by_the_deploy[env.strip()] = setting.strip()
    return set_by_the_deploy


def _bash_array(script: str, name: str):
    match = re.search(rf"^{name}=\(\s*\n(.*?)^\)", script, re.S | re.M)
    return None if match is None else {line.strip() for line in match.group(1).splitlines() if line.strip()}


def test_the_code_reads_some_secrets():
    # Guards the scan itself: if it found nothing, the tests below would pass for nothing.
    assert {"FULLENRICH_API_KEY", "MILLIONVERIFIER_API_KEY", "HUBSPOT_WEBHOOK_SECRET"} <= _secrets_read_by_the_code()


def test_every_secret_the_code_reads_is_mounted_by_the_deploy():
    missing = _secrets_read_by_the_code() - set(_secrets_mounted_by_the_deploy())
    assert not missing, f"read by the code but not mounted by cloudbuild.yaml (each merge would switch it off): {sorted(missing)}"


def test_each_mounted_secret_has_the_same_name_as_its_variable():
    for env, secret in _secrets_mounted_by_the_deploy().items():
        assert env == secret, f"{env} is mounted from a differently named secret {secret}"


def test_the_runtime_account_can_read_exactly_the_secrets_the_deploy_mounts():
    script = (ROOT / "setup_runtime_service_account.sh").read_text()
    granted = _bash_array(script, "SECRETS")
    assert granted is not None
    assert granted == set(_secrets_mounted_by_the_deploy().values())


def test_a_secret_the_deploy_mounts_is_not_left_in_the_optional_list():
    script = (ROOT / "setup_runtime_service_account.sh").read_text()
    optional = _bash_array(script, "OPTIONAL_SECRETS") or set()
    assert not optional & set(_secrets_mounted_by_the_deploy().values())


@pytest.mark.parametrize("secret", ["BOUNCEBAN_API_KEY", "SEAMLESS_API_KEY"])
def test_the_vendor_keys_that_were_lost_on_2026_10_25_are_mounted(secret):
    assert _secrets_mounted_by_the_deploy().get(secret) == secret


def test_the_secret_the_apps_function_signs_with_is_mounted():
    # The app's cards reach /app/... through a function that signs each request with this secret; the service refuses
    # (503) every one of them until it has it, and each merge's deploy would take it away again if it were not listed.
    assert _secrets_mounted_by_the_deploy().get("APP_REQUEST_SECRET") == "APP_REQUEST_SECRET"


def test_the_service_only_takes_app_requests_from_the_portal_it_enriches():
    # With HUBSPOT_PORTAL_ID unset the service accepts a signed request from any HubSpot account that holds the secret,
    # and then looks the record ids up in the one portal its own token belongs to: Covr_HubSpot, 24434722.
    assert _env_vars_set_by_the_deploy().get("HUBSPOT_PORTAL_ID") == "24434722"


def test_the_env_var_scan_sees_the_settings_the_deploy_already_makes():
    # Guards the scan itself, as test_the_code_reads_some_secrets does for the secrets.
    assert _env_vars_set_by_the_deploy().get("CLOUD_TASKS_QUEUE") == "hubspot-enrichment"
