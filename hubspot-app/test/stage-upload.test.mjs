// scripts/stage-upload.mjs: how the app gets to a HubSpot account without the wrong thing travelling.
//
// `hs project upload` zips what is under the folder it runs in and silently skips any path with a hidden folder in it
// (our worktree lives under .claude/, so the first try made a 22-byte zip). The script uploads from a staged copy of
// exactly the files git tracks under hsproject.json and src/, and is built so a slip cannot reach an account: the
// account is always named (HubSpot's CLI has a default, and it is Covr_HubSpot), it is a dry run unless the account is
// typed twice, and it refuses to apply when what is on disk is not what is committed or when the function's secret is
// not in the account yet. These tests run the real script against throwaway git repos, a fake `hs`, and (once) the
// real repo in a dry run. Nothing here talks to HubSpot.
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';
import { SERVICE_PORTAL, gitState, parseArgs, run, stage } from '../scripts/stage-upload.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const REPO = join(here, '..', '..');
const SCRIPT = join(here, '..', 'scripts', 'stage-upload.mjs');
const PROD = '24434722';

const made = [];
const scratch = (prefix = 'stage-test-') => {
  const dir = mkdtempSync(join(tmpdir(), prefix));
  made.push(dir);
  return dir;
};
afterEach(() => {
  while (made.length) rmSync(made.pop(), { recursive: true, force: true });
});

// What the app folder holds: what ships, and what must not.
const APP_FILES = {
  'hsproject.json': '{"name":"covr-enrichment-app","srcDir":"src"}',
  'src/app/app-hsmeta.json': '{"uid":"covr_enrichment_app"}',
  'src/app/cards/Enrichment.tsx': 'export const Card = () => null;\n',
  'src/app/cards/package.json': '{"dependencies":{}}',
  'src/app/functions/service.js': "exports.main = async () => ({ ok: true });\n",
  'README.md': 'docs',
  'test/card.test.tsx': 'test',
};

function writeFiles(root, files) {
  for (const [rel, text] of Object.entries(files)) {
    mkdirSync(dirname(join(root, rel)), { recursive: true });
    writeFileSync(join(root, rel), text);
  }
}

const GIT_ENV = {
  ...process.env,
  GIT_AUTHOR_NAME: 'test', GIT_AUTHOR_EMAIL: 'test@example.com', GIT_COMMITTER_NAME: 'test', GIT_COMMITTER_EMAIL: 'test@example.com',
};

// A git repo with the app one folder down, as hubspot-app/ sits in the real repo, committed once.
function gitRepo(files = APP_FILES) {
  const root = scratch('stage-repo-');
  const app = join(root, 'hubspot-app');
  writeFiles(app, files);
  writeFileSync(join(root, '.gitignore'), 'node_modules/\n');
  const git = (...args) => execFileSync('git', ['-C', root, '-c', 'commit.gpgsign=false', ...args], { env: GIT_ENV, encoding: 'utf8' });
  git('init', '-q');
  git('add', '-A');
  git('commit', '-q', '-m', 'first');
  return { root, app, git };
}

const walk = (root, dir = root) => readdirSync(dir).flatMap((name) => {
  const path = join(dir, name);
  return statSync(path).isDirectory() ? walk(root, path) : [relative(root, path)];
});

// A fake `hs`: records every call, answers `secret list` from `secrets`, and uploads with `uploadStatus`.
function fakeHs({ secrets = ['APP_REQUEST_SECRET'], listStatus = 0, uploadStatus = 0 } = {}) {
  const calls = [];
  const spawn = (cmd, args, opts) => {
    calls.push({ cmd, args, opts });
    if (args[0] === 'secret') return { status: listStatus, stdout: `Secrets for account X:\n${secrets.map((s) => `  ${s}`).join('\n')}\n`, stderr: '' };
    return { status: uploadStatus };
  };
  return { calls, spawn };
}

// The tests that run the script as a real process get a PATH whose `hs` is a stub that only writes down it was called and
// fails. Whatever a bug (or a mutation) makes the script do, it cannot reach HubSpot from here.
function stubbedHs() {
  const bin = scratch('stage-bin-');
  const marker = join(bin, 'hs-was-called');
  writeFileSync(join(bin, 'hs'), `#!/bin/sh\necho "$@" >> "${marker}"\nexit 99\n`, { mode: 0o755 });
  return { env: { ...process.env, PATH: `${bin}:${process.env.PATH}` }, called: () => existsSync(marker) };
}

const upload = (calls) => calls.filter((c) => c.args[0] === 'project');
const logger = () => {
  const lines = [];
  return { lines, log: (line = '') => lines.push(String(line)), text: () => lines.join('\n') };
};

// ── The arguments ─────────────────────────────────────────────────────────────

describe('parseArgs', () => {
  it('refuses to run without an account and says there is no default', () => {
    expect(() => parseArgs([])).toThrow(/--account.*no default/i);
    expect(() => parseArgs(['--apply', '--confirm', PROD])).toThrow(/--account/);
  });

  it('refuses an account that is not digits', () => {
    expect(() => parseArgs(['--account', 'covr'])).toThrow(/digits/);
    expect(() => parseArgs(['--account', '--apply'])).toThrow(/digits/);
    expect(() => parseArgs(['--account', '24434722; rm -rf'])).toThrow(/digits/);
  });

  it('is a dry run unless asked to apply', () => {
    expect(parseArgs(['--account', PROD])).toEqual({ account: PROD, apply: false, confirm: undefined });
  });

  it('applies only when the account is typed a second time, and the same', () => {
    expect(() => parseArgs(['--account', PROD, '--apply'])).toThrow(/--confirm/);
    expect(() => parseArgs(['--account', PROD, '--apply', '--confirm', '52218377'])).toThrow(/--confirm/);
    expect(parseArgs(['--account', PROD, '--apply', '--confirm', PROD])).toMatchObject({ account: PROD, apply: true, confirm: PROD });
  });

  it('refuses an option it does not know, so a typo cannot quietly leave the dry run', () => {
    expect(() => parseArgs(['--account', PROD, '--aply', '--confirm', PROD])).toThrow(/Unknown option.*--aply/);
    expect(() => parseArgs(['--account', PROD, PROD])).toThrow(/Unexpected/);
  });
});

// ── What goes into the staged copy ────────────────────────────────────────────

describe('stage', () => {
  it('copies the project file and src/, byte for byte, and nothing else', () => {
    const app = scratch('stage-app-');
    writeFiles(app, APP_FILES);
    const out = scratch('stage-out-');

    const staged = stage({ appDir: app, outDir: out, files: Object.keys(APP_FILES) });

    expect(walk(staged.dir).sort()).toEqual(
      ['hsproject.json', 'src/app/app-hsmeta.json', 'src/app/cards/Enrichment.tsx', 'src/app/cards/package.json', 'src/app/functions/service.js'].sort(),
    );
    expect(readFileSync(join(staged.dir, 'src/app/functions/service.js'), 'utf8')).toBe(APP_FILES['src/app/functions/service.js']);
    expect(staged.files).toEqual(walk(staged.dir).sort());
  });

  it('leaves out installed packages, hidden files and anything outside the project even when handed them', () => {
    const app = scratch('stage-app-');
    writeFiles(app, {
      ...APP_FILES,
      'src/app/cards/node_modules/react/index.js': 'x',
      'src/app/.DS_Store': 'x',
      '.hsaccount': '52218377',
    });

    const staged = stage({ appDir: app, outDir: scratch('stage-out-'), files: [...Object.keys(APP_FILES), 'src/app/cards/node_modules/react/index.js', 'src/app/.DS_Store', '.hsaccount'] });

    expect(staged.files.some((f) => f.includes('node_modules') || f.includes('.DS_Store') || f.includes('.hsaccount'))).toBe(false);
    expect(staged.files).not.toContain('README.md');
    expect(staged.files.some((f) => f.startsWith('test/'))).toBe(false);
  });

  it('refuses a staging folder with a hidden folder in its path, which the CLI would zip as nothing', () => {
    const app = scratch('stage-app-');
    writeFiles(app, APP_FILES);
    const hidden = join(scratch('stage-out-'), '.claude', 'worktree');

    expect(() => stage({ appDir: app, outDir: hidden, files: Object.keys(APP_FILES) })).toThrow(/hidden folder/);
    expect(existsSync(hidden)).toBe(false);
  });

  it('refuses a copy that does not hold the project file and the app file, rather than upload an empty zip', () => {
    const app = scratch('stage-app-');
    writeFiles(app, APP_FILES);

    expect(() => stage({ appDir: app, outDir: scratch('stage-out-'), files: [] })).toThrow(/hsproject\.json/);
    expect(() => stage({ appDir: app, outDir: scratch('stage-out-'), files: ['hsproject.json'] })).toThrow(/app-hsmeta\.json/);
  });

  it('refuses a path that climbs out of the app folder', () => {
    const app = scratch('stage-app-');
    writeFiles(app, APP_FILES);

    expect(() => stage({ appDir: app, outDir: scratch('stage-out-'), files: [...Object.keys(APP_FILES), '../secrets.txt'] })).toThrow(/outside/);
  });
});

// ── What git says is there ────────────────────────────────────────────────────

describe('gitState', () => {
  it('lists the tracked files under the project file and src/, and not installed packages or other folders', () => {
    const { app } = gitRepo();
    writeFiles(app, { 'src/app/cards/node_modules/react/index.js': 'x' });

    const state = gitState(app);

    expect(state.files.sort()).toEqual(
      ['hsproject.json', 'src/app/app-hsmeta.json', 'src/app/cards/Enrichment.tsx', 'src/app/cards/package.json', 'src/app/functions/service.js'].sort(),
    );
    expect(state.dirty).toEqual([]);
    expect(state.commit).toMatch(/^[0-9a-f]{40}$/);
  });

  it('calls the tree dirty when something that ships is edited or added but not committed', () => {
    const { app } = gitRepo();
    writeFiles(app, { 'src/app/functions/service.js': '// edited\n' });
    expect(gitState(app).dirty.join('\n')).toMatch(/service\.js/);

    const second = gitRepo();
    writeFiles(second.app, { 'src/app/cards/New.tsx': 'export {}' });
    expect(gitState(second.app).dirty.join('\n')).toMatch(/New\.tsx/);
  });

  it('does not count edits to what never ships', () => {
    const { app } = gitRepo();
    writeFiles(app, { 'README.md': 'changed', 'test/card.test.tsx': 'changed' });

    expect(gitState(app).dirty).toEqual([]);
  });

  it('stops with a message when git cannot read the folder, rather than stage a guess', () => {
    const notARepo = scratch('stage-nogit-');
    writeFiles(notARepo, APP_FILES);

    expect(() => gitState(notARepo)).toThrow(/git could not read/);
  });

  it('says whether the commit is on origin/main', () => {
    const { app, git } = gitRepo();
    expect(gitState(app).onMain).toBe(false);

    git('update-ref', 'refs/remotes/origin/main', 'HEAD');
    expect(gitState(app).onMain).toBe(true);
  });
});

// ── The run ───────────────────────────────────────────────────────────────────

describe('run: a dry run', () => {
  it('stages the files, lists them and the command it would run, and sends nothing', () => {
    const { app } = gitRepo();
    const hs = fakeHs();
    const out = logger();

    const result = run({ argv: ['--account', PROD], appDir: app, outDir: scratch('stage-out-'), spawn: hs.spawn, log: out.log });

    expect(result.applied).toBe(false);
    expect(hs.calls).toEqual([]);
    expect(out.text()).toMatch(/Dry run/);
    expect(out.text()).toContain('src/app/functions/service.js');
    expect(out.text()).toContain(`hs project upload --account ${PROD}`);
    expect(out.text()).toMatch(/Nothing was sent/);
    expect(existsSync(join(result.dir, 'hsproject.json'))).toBe(true);
  });

  it('shows what apply would refuse, without refusing', () => {
    const { app } = gitRepo();
    writeFiles(app, { 'src/app/functions/service.js': '// edited\n' });
    const out = logger();

    const result = run({ argv: ['--account', PROD], appDir: app, outDir: scratch('stage-out-'), spawn: fakeHs().spawn, log: out.log });

    expect(result.applied).toBe(false);
    expect(out.text()).toMatch(/not committed/i);
    expect(out.text()).toMatch(/service\.js/);
  });

  it('survives a tracked file deleted but not committed, leaves it out of the copy and names it', () => {
    const { app } = gitRepo();
    rmSync(join(app, 'src/app/cards/Enrichment.tsx'));
    const out = logger();

    const result = run({ argv: ['--account', PROD], appDir: app, outDir: scratch('stage-out-'), spawn: fakeHs().spawn, log: out.log });

    expect(result.files).not.toContain('src/app/cards/Enrichment.tsx');
    expect(out.text()).toMatch(/Not committed[\s\S]*Enrichment\.tsx/);
  });

  it('says which commit it staged and whether that commit is on origin/main', () => {
    const { app, git } = gitRepo();
    const commit = git('rev-parse', 'HEAD').trim();
    const before = logger();
    run({ argv: ['--account', PROD], appDir: app, outDir: scratch('stage-out-'), spawn: fakeHs().spawn, log: before.log });
    expect(before.text()).toContain(commit.slice(0, 7));
    expect(before.text()).toMatch(/not on origin\/main/i);

    git('update-ref', 'refs/remotes/origin/main', 'HEAD');
    const after = logger();
    run({ argv: ['--account', PROD], appDir: app, outDir: scratch('stage-out-'), spawn: fakeHs().spawn, log: after.log });
    expect(after.text()).toMatch(/on origin\/main/i);
    expect(after.text()).not.toMatch(/not on origin\/main/i);
  });

  it('warns that the service takes requests from one portal only when the account is another', () => {
    const { app } = gitRepo();
    const other = logger();
    run({ argv: ['--account', '52218377'], appDir: app, outDir: scratch('stage-out-'), spawn: fakeHs().spawn, log: other.log });
    expect(other.text()).toContain(SERVICE_PORTAL);
    expect(other.text()).toMatch(/refused/i);

    const prod = logger();
    run({ argv: ['--account', SERVICE_PORTAL], appDir: app, outDir: scratch('stage-out-'), spawn: fakeHs().spawn, log: prod.log });
    expect(prod.text()).not.toMatch(/refused/i);
  });

  it('reminds that apply checks for the function\'s secret first', () => {
    const { app } = gitRepo();
    const out = logger();

    run({ argv: ['--account', PROD], appDir: app, outDir: scratch('stage-out-'), spawn: fakeHs().spawn, log: out.log });

    expect(out.text()).toContain('APP_REQUEST_SECRET');
  });
});

describe('run: applying', () => {
  const apply = ['--account', PROD, '--apply', '--confirm', PROD];

  it('checks the account has the secret, then uploads from the staged copy with the terminal handed over', () => {
    const { app } = gitRepo();
    const hs = fakeHs();
    const out = logger();

    const result = run({ argv: apply, appDir: app, outDir: scratch('stage-out-'), spawn: hs.spawn, log: out.log });

    expect(result.applied).toBe(true);
    expect(hs.calls.map((c) => [c.cmd, ...c.args])).toEqual([
      ['hs', 'secret', 'list', '--account', PROD],
      ['hs', 'project', 'upload', '--account', PROD],
    ]);
    expect(upload(hs.calls)[0].opts).toMatchObject({ cwd: result.dir, stdio: 'inherit' });
    expect(existsSync(join(upload(hs.calls)[0].opts.cwd, 'hsproject.json'))).toBe(true);
  });

  it('never leaves the account out of an hs call', () => {
    const { app } = gitRepo();
    const hs = fakeHs();

    run({ argv: ['--account', '52218377', '--apply', '--confirm', '52218377'], appDir: app, outDir: scratch('stage-out-'), spawn: hs.spawn, log: logger().log });

    for (const call of hs.calls) {
      expect(call.args).toContain('--account');
      expect(call.args[call.args.indexOf('--account') + 1]).toBe('52218377');
    }
  });

  it('refuses when what ships is not committed, and uploads nothing', () => {
    const { app } = gitRepo();
    writeFiles(app, { 'src/app/functions/service.js': '// edited\n' });
    const hs = fakeHs();

    expect(() => run({ argv: apply, appDir: app, outDir: scratch('stage-out-'), spawn: hs.spawn, log: logger().log })).toThrow(/not committed/i);
    expect(hs.calls).toEqual([]);
  });

  it('refuses when the account does not hold the secret, says how to add it, and uploads nothing', () => {
    const { app } = gitRepo();
    const hs = fakeHs({ secrets: ['HUBSPOT_CLIENT_SECRET', 'OLD_APP_REQUEST_SECRET_2'] });

    expect(() => run({ argv: apply, appDir: app, outDir: scratch('stage-out-'), spawn: hs.spawn, log: logger().log })).toThrow(
      new RegExp(`hs secret add APP_REQUEST_SECRET --account ${PROD}`),
    );
    expect(upload(hs.calls)).toEqual([]);
  });

  it('refuses when it cannot read the account\'s secrets, rather than guess', () => {
    const { app } = gitRepo();
    const hs = fakeHs({ listStatus: 1 });

    expect(() => run({ argv: apply, appDir: app, outDir: scratch('stage-out-'), spawn: hs.spawn, log: logger().log })).toThrow(/could not read/i);
    expect(upload(hs.calls)).toEqual([]);
  });

  it('stops with the upload\'s failure and does not print what to do next', () => {
    const { app } = gitRepo();
    const hs = fakeHs({ uploadStatus: 1 });
    const out = logger();

    expect(() => run({ argv: apply, appDir: app, outDir: scratch('stage-out-'), spawn: hs.spawn, log: out.log })).toThrow(/upload failed/i);
    expect(out.text()).not.toMatch(/install-app/);
  });

  it('says what to do after the upload: install the app, then put the cards on a view', () => {
    const { app } = gitRepo();
    const out = logger();

    run({ argv: apply, appDir: app, outDir: scratch('stage-out-'), spawn: fakeHs().spawn, log: out.log });

    expect(out.text()).toContain(`hs project install-app --account ${PROD}`);
    expect(out.text()).toContain(`hs project app-install-status --account ${PROD}`);
    expect(out.text()).toMatch(/Record customization/);
    expect(out.text()).toMatch(/Super Admin/);
  });
});

// ── It is tied to the rest of the repo ────────────────────────────────────────

describe('the repo', () => {
  it('names the same portal as the deploy, the one the service accepts', () => {
    const deploy = readFileSync(join(REPO, 'cloudbuild.yaml'), 'utf8');

    expect(deploy.match(/HUBSPOT_PORTAL_ID=(\d+)/)?.[1]).toBe(SERVICE_PORTAL);
  });

  it('stages the real app: the project file, the card and function files, and no packages or tests', () => {
    const hs = stubbedHs();
    const out = execFileSync('node', [SCRIPT, '--account', PROD], { encoding: 'utf8', env: hs.env });

    expect(hs.called()).toBe(false);
    expect(out).toContain('hsproject.json');
    expect(out).toContain('src/app/app-hsmeta.json');
    expect(out).toContain('src/app/functions/service.js');
    expect(out).toContain('src/app/cards/EnrichmentView.tsx');
    expect(out).not.toMatch(/node_modules|\.test\./);
    expect(out).toMatch(/Nothing was sent/);
  });

  it('exits with a message and a failure when run without an account', () => {
    const hs = stubbedHs();
    let failure;
    try {
      execFileSync('node', [SCRIPT], { encoding: 'utf8', stdio: 'pipe', env: hs.env });
    } catch (err) {
      failure = err;
    }

    expect(hs.called()).toBe(false);
    expect(failure?.status).toBe(1);
    expect(String(failure?.stderr)).toMatch(/STOPPED.*--account/s);
  });
});
