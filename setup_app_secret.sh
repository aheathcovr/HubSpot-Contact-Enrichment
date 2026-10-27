#!/usr/bin/env bash
# The secret the HubSpot app's function signs its requests with, and the enrichment service checks them against.
#
# A static-auth app has no client secret, so HubSpot cannot sign a card's requests. The cards call the app's function
# (hubspot-app/src/app/functions/service.js), which signs each request to the service with a secret the two share. That
# secret is one random string that must end up in two places:
#
#   1. Secret Manager, as APP_REQUEST_SECRET, for the service (the runtime account is let read it). This script does it.
#   2. HubSpot, as a secret of the app's function, in every HubSpot account the app is uploaded to. HubSpot's CLI takes
#      a secret only at a hidden prompt, so this script puts the value on your clipboard: run the `hs secret add`
#      command it prints, paste at the prompt, and press Enter here, which clears the clipboard.
#
# The value is made here, on your machine, and is never printed, never written to a file and never on a command line.
#
# Not part of this script, each needing its own go-ahead: adding the secret to cloudbuild.yaml
# (--set-secrets=APP_REQUEST_SECRET=APP_REQUEST_SECRET:latest, which must wait until the secret exists or a deploy
# fails to start), moving traffic to a build that has it, and uploading the app.
#
# DRY RUN BY DEFAULT: reads the project's current state, prints every command it would run, changes nothing.
#
#   ./setup_app_secret.sh                        # show what would run
#   APPLY=1 ./setup_app_secret.sh                # make the secret, let the service read it, copy it for HubSpot
#   APPLY=1 COPY_EXISTING=1 ./setup_app_secret.sh   # the secret exists: copy its value again (for another HubSpot account)
#
# Safe to re-run: it never replaces or deletes a secret. To rotate, add a new version yourself, then update the secret
# in HubSpot (`hs secret update`) in every account, in the same sitting: until both match, requests are refused.
#
# Needs a logged-in gcloud (gcloud auth login) with permission to create secrets and edit their access, and macOS's
# pbcopy for the clipboard.

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-gen-lang-client-0844868008}"
SECRET_NAME="APP_REQUEST_SECRET"
RUNTIME_SA="hubspot-enrichment-runtime@${PROJECT_ID}.iam.gserviceaccount.com"
HUBSPOT_ACCOUNT="${HUBSPOT_ACCOUNT:-52218377}"      # the account to add the secret to (the test account first)
APPLY="${APPLY:-0}"
COPY_EXISTING="${COPY_EXISTING:-0}"

# Print a command the way you could paste it; run it only when APPLY=1.
run() {
  if [[ "${APPLY}" == "1" ]]; then
    "$@"
  else
    printf '  +'
    for a in "$@"; do
      case "$a" in *[\ \"\&\(\)\<\>\|\;]*) printf " '%s'" "$a" ;; *) printf ' %s' "$a" ;; esac
    done
    printf '\n'
  fi
}

mode() { [[ "${APPLY}" == "1" ]] && echo "APPLYING" || echo "DRY RUN (nothing is changed)"; }

# Put stdin on the clipboard, wait while you paste it into HubSpot, then clear the clipboard.
hand_over() {
  pbcopy
  echo ""
  echo "    The value is on your clipboard. In another terminal run:"
  echo ""
  echo "        hs secret add ${SECRET_NAME} --account ${HUBSPOT_ACCOUNT}"
  echo ""
  echo "    and paste at the hidden prompt. Then come back here and press Enter to clear the clipboard."
  # From the terminal, not from stdin: stdin here is the pipe that carried the value, which is already empty. (The tests
  # point CONFIRM_FROM at something else.)
  read -r _ < "${CONFIRM_FROM:-/dev/tty}" || true
  printf '' | pbcopy
  echo "    Clipboard cleared."
}

echo "==> $(mode)"
echo "==> Project: ${PROJECT_ID}"
echo "==> Secret:  ${SECRET_NAME} (read by ${RUNTIME_SA})"
echo ""

# An error here (not logged in, no access) stops the script before anything is created: set -e on the substitution.
existing="$(gcloud secrets list --project="${PROJECT_ID}" --filter="name:${SECRET_NAME}" --format="value(name)")"

if [[ -n "${existing}" ]]; then
  echo "--- ${SECRET_NAME}"
  echo "    Already exists. It is not replaced."
  if [[ "${COPY_EXISTING}" == "1" ]]; then
    if [[ "${APPLY}" == "1" ]]; then
      gcloud secrets versions access latest --secret="${SECRET_NAME}" --project="${PROJECT_ID}" | hand_over
    else
      echo "    (COPY_EXISTING=1: with APPLY=1 its value would be copied to your clipboard for HubSpot.)"
    fi
  else
    echo "    To add it to another HubSpot account, run again with APPLY=1 COPY_EXISTING=1."
  fi
  exit 0
fi

echo "--- ${SECRET_NAME}"
if [[ "${APPLY}" == "1" ]]; then
  value="$(openssl rand -base64 32 | tr -d '\n')"
  printf '%s' "${value}" | gcloud secrets create "${SECRET_NAME}" --project="${PROJECT_ID}" --replication-policy=automatic --data-file=-
  gcloud secrets add-iam-policy-binding "${SECRET_NAME}" --project="${PROJECT_ID}" \
    --member="serviceAccount:${RUNTIME_SA}" --role="roles/secretmanager.secretAccessor" --condition=None --quiet --format=none
  echo "    Created, and the service's runtime account can read it."
  printf '%s' "${value}" | hand_over
  unset value
else
  echo "    A random value would be made here and stored with:"
  run gcloud secrets create "${SECRET_NAME}" --project="${PROJECT_ID}" --replication-policy=automatic --data-file=-
  run gcloud secrets add-iam-policy-binding "${SECRET_NAME}" --project="${PROJECT_ID}" \
    --member="serviceAccount:${RUNTIME_SA}" --role="roles/secretmanager.secretAccessor" --condition=None --quiet --format=none
  echo "    Then it would be copied to your clipboard for: hs secret add ${SECRET_NAME} --account ${HUBSPOT_ACCOUNT}"
fi

echo ""
echo "Next, each needing its own go-ahead: add --set-secrets=${SECRET_NAME}=${SECRET_NAME}:latest to cloudbuild.yaml,"
echo "deploy a build from main, move traffic to it, and upload the app."
