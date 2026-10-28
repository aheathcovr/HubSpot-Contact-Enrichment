// Uploads the Covr Enrichment app to a HubSpot account from a staged copy, so that what travels is exactly what is
// committed. Modelled on the sales-handoff repo's stage-upload.mjs, which put its app into Covr_HubSpot.
//
//   node hubspot-app/scripts/stage-upload.mjs --account 24434722
//       A dry run: stages the copy, lists it and the command it would run. Sends nothing, not even a read.
//   node hubspot-app/scripts/stage-upload.mjs --account 24434722 --apply --confirm 24434722
//       Checks the account holds APP_REQUEST_SECRET (the function names it), then runs
//       `hs project upload --account 24434722` from the staged copy, with this terminal handed over so HubSpot's own
//       prompt (creating the project in an account that has none) is answered by you.
//
// Why a staged copy: the CLI skips any path with a hidden folder in it (this repo's worktrees live under .claude/, so
// a direct upload zipped nothing), and a copy of the files git tracks leaves out node_modules, tests and the rest.
// Why --account has no default: the CLI's default account is Covr_HubSpot. It refuses to apply when what ships is not
// committed, so the upload is always a commit that can be named.
//
// After the upload the app still has to be installed (`hs project install-app`) and its cards put on a record view:
// the script prints both. Run from anywhere; the app folder is found from this file's own location.
import { spawnSync } from 'node:child_process';
import { cpSync, existsSync, mkdirSync, mkdtempSync, readdirSync, realpathSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

// The one portal the service answers /app requests from: HUBSPOT_PORTAL_ID in cloudbuild.yaml (the test checks they match).
export const SERVICE_PORTAL = '24434722';
const SECRET = 'APP_REQUEST_SECRET';
const APP_DIR = fileURLToPath(new URL('..', import.meta.url));
// What ships: the project file and everything under src/.
const SHIPPED = ['hsproject.json', 'src'];
const REQUIRED = ['hsproject.json', 'src/app/app-hsmeta.json'];

const isHidden = (path) => path.split(/[\\/]/).some((part) => part.startsWith('.') && part !== '.' && part !== '..');

export function parseArgs(argv) {
  const values = {};
  let apply = false;
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--account' || arg === '--confirm') {
      values[arg] = argv[i + 1];
      i += 1;
    } else if (arg === '--apply') {
      apply = true;
    } else if (arg.startsWith('--')) {
      throw new Error(`Unknown option: ${arg}`);
    } else {
      throw new Error(`Unexpected argument: ${arg}`);
    }
  }
  const account = values['--account'];
  if (account === undefined) throw new Error('Give --account <HubSpot account id>, digits only. There is no default.');
  if (!/^\d+$/.test(account)) throw new Error('--account must be digits only: the HubSpot account id.');
  const confirm = values['--confirm'];
  if (apply && confirm !== account) throw new Error(`Applying needs --confirm ${account}: type the account id a second time.`);
  return { account, apply, confirm };
}

const walk = (root, dir = root) => readdirSync(dir).flatMap((name) => {
  const path = join(dir, name);
  return statSync(path).isDirectory() ? walk(root, path) : [relative(root, path)];
});

// Copies the files that ship into <outDir>/hubspot-app. `files` are paths relative to appDir, as git lists them.
export function stage({ appDir = APP_DIR, outDir, files }) {
  for (const rel of files) {
    if (rel.startsWith('/') || rel.split('/').includes('..')) throw new Error(`${rel} is outside the app folder.`);
  }
  const kept = files.filter((rel) => (
    !rel.split('/').some((part) => part.startsWith('.') || part === 'node_modules') && (rel === 'hsproject.json' || rel.startsWith('src/'))
  ));
  for (const required of REQUIRED) {
    if (!kept.includes(required)) throw new Error(`The staged copy has no ${required}, so HubSpot would get an empty project.`);
  }
  const dir = join(outDir ?? mkdtempSync(join(tmpdir(), 'covr-enrichment-stage-')), 'hubspot-app');
  if (isHidden(dir)) {
    throw new Error(`The staging folder ${dir} has a hidden folder in its path. The HubSpot CLI skips those and would zip nothing.`);
  }
  mkdirSync(dir, { recursive: true });
  for (const rel of kept) {
    mkdirSync(dirname(join(dir, rel)), { recursive: true });
    cpSync(join(appDir, rel), join(dir, rel));
  }
  return { dir, files: walk(dir).sort() };
}

const realGit = (appDir, args) => {
  const done = spawnSync('git', ['-C', appDir, ...args], { encoding: 'utf8' });
  return { status: done.status ?? 1, stdout: done.stdout ?? '', stderr: done.stderr ?? '' };
};
const realSpawn = (cmd, args, opts) => spawnSync(cmd, args, opts);

// What git says about what ships: the tracked files, anything edited or added but not committed, the commit, and
// whether that commit is on origin/main (as last fetched).
export function gitState(appDir = APP_DIR, git = realGit) {
  const listed = git(appDir, ['ls-files', '-z', '--', ...SHIPPED]);
  const status = git(appDir, ['status', '--porcelain', '--', ...SHIPPED]);
  const head = git(appDir, ['rev-parse', 'HEAD']);
  for (const step of [listed, status, head]) {
    if (step.status !== 0) throw new Error(`git could not read the app folder ${appDir}: ${step.stderr.trim()}`);
  }
  return {
    commit: head.stdout.trim(),
    files: listed.stdout.split('\0').filter(Boolean),
    dirty: status.stdout.split('\n').filter(Boolean),
    onMain: git(appDir, ['merge-base', '--is-ancestor', 'HEAD', 'origin/main']).status === 0,
  };
}

function requireSecret(account, spawn) {
  const listed = spawn('hs', ['secret', 'list', '--account', account], { encoding: 'utf8' });
  if (listed.status !== 0) throw new Error(`Could not read the secrets in account ${account} (hs exited ${listed.status}). Nothing was uploaded.`);
  const names = String(listed.stdout).split('\n').map((line) => line.trim());
  if (!names.includes(SECRET)) {
    throw new Error(`Account ${account} has no ${SECRET}, which the app's function signs with. Add it first, pasting at the hidden prompt:\n  hs secret add ${SECRET} --account ${account}`);
  }
}

export function run({ argv, appDir = APP_DIR, outDir, git = realGit, spawn = realSpawn, log = console.log }) {
  const { account, apply } = parseArgs(argv);
  const state = gitState(appDir, git);
  if (apply && state.dirty.length) {
    throw new Error(`What ships is not committed, so nothing was uploaded. Commit it first:\n${state.dirty.map((line) => `  ${line}`).join('\n')}`);
  }
  if (apply) requireSecret(account, spawn);

  // A tracked file deleted but not committed is in git's list and not on disk; the dirty list above names it.
  const staged = stage({ appDir, outDir, files: state.files.filter((rel) => existsSync(join(appDir, rel))) });
  const upload = `hs project upload --account ${account}`;
  log(`${apply ? 'Upload' : 'Dry run'}: account ${account}.`);
  log(`Commit ${state.commit.slice(0, 7)} ${state.onMain ? 'is on origin/main' : 'is not on origin/main (as last fetched)'}.`);
  if (!apply && state.dirty.length) {
    log(`Not committed, so apply would stop:\n${state.dirty.map((line) => `  ${line}`).join('\n')}`);
  }
  if (account !== SERVICE_PORTAL) {
    log(`Account ${account} is not ${SERVICE_PORTAL}. The service takes /app requests from ${SERVICE_PORTAL} only (HUBSPOT_PORTAL_ID in cloudbuild.yaml), so the cards would be refused there.`);
  }
  log(`Staged copy: ${staged.dir}`);
  for (const file of staged.files) log(`  ${file}`);

  if (!apply) {
    log(`\nNothing was sent. Apply first checks the account holds ${SECRET}, then runs, from the staged copy:\n  ${upload}`);
    log(`To apply, run this again with --apply --confirm ${account}.`);
    return { applied: false, dir: staged.dir, files: staged.files };
  }

  log(`\nRunning: ${upload}`);
  const done = spawn('hs', ['project', 'upload', '--account', account], { cwd: staged.dir, stdio: 'inherit' });
  if (done.status !== 0) throw new Error(`The upload failed (hs exited ${done.status}). Nothing here retries it.`);
  log(`\nUploaded. Now, in this order:
  1. Install the app (a Super Admin approves its scopes):  hs project install-app --account ${account}
  2. Check it:  hs project app-install-status --account ${account}
  3. Put the cards on a record view: Settings > Objects > Companies (the Enrichment card) and Contacts (the Contact check
     card) > Record customization, on a view for a few people first. A card is invisible until it is on a view.`);
  return { applied: true, dir: staged.dir, files: staged.files };
}

if (process.argv[1] && realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url))) {
  try {
    run({ argv: process.argv.slice(2) });
  } catch (err) {
    console.error(`\nSTOPPED: ${err.message}`);
    process.exit(1);
  }
}
