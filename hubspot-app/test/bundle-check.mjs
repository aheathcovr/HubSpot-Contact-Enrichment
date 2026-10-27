// Builds the card with HubSpot's own extension build (@hubspot/ui-extensions-dev-server, the build behind
// `hs project dev` and uploads) and checks the bundle. The cards are copied to a temp folder, away from any other
// tsconfig.json, with node_modules linked to the packages installed here.
import { cpSync, existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, symlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildSingleExtension } from '@hubspot/ui-extensions-dev-server/build';

const here = (p) => fileURLToPath(new URL(p, import.meta.url));
const cards = here('../src/app/cards');
const temp = mkdtempSync(join(tmpdir(), 'enrichment-card-'));
const work = join(temp, 'cards');
const out = join(temp, 'dist');
const problems = [];
try {
  cpSync(cards, work, { recursive: true, filter: (src) => !src.includes('node_modules') });
  symlinkSync(here('./node_modules'), join(work, 'node_modules'), 'dir');

  const metas = readdirSync(cards).filter((f) => f.endsWith('-hsmeta.json'));
  if (metas.length < 2) problems.push(`Expected the two card configs, found ${metas.length}.`);
  for (const file of metas) {
    const meta = JSON.parse(readFileSync(join(cards, file), 'utf8'));
    const entry = meta.config.entrypoint.replace('/app/cards/', '');
    if (!existsSync(join(cards, entry))) {
      problems.push(`${file}: entrypoint ${entry} is missing`);
      continue;
    }
    await buildSingleExtension({ file: entry, outputDir: out, root: work, minify: true, logLevel: 'warn', codeCheckingPluginEnabled: true });
    const bundle = readFileSync(join(out, entry.replace(/\.tsx$/, '.js')), 'utf8');
    if (bundle.includes('React.createElement')) problems.push(`${entry}: the bundle uses React.createElement without importing React. Keep tsconfig.json ("jsx": "react-jsx") next to the cards.`);
    // What each card must call: the company card an app function, the contact card the enrichment service.
    const calls = {
      'Enrichment.tsx': ['"enrichment"', 'hubspot-enrichment-kpqza7tiga-uc.a.run.app/app', '/company/status', '/company/run', '/company/find', '/company/add', '/company/candidates'],
      'ContactCheck.tsx': ['hubspot-enrichment-kpqza7tiga-uc.a.run.app/app', '/contact/status', '/contact/run'],
    }[entry];
    if (!calls) problems.push(`${entry}: no check is defined for this card in bundle-check.mjs.`);
    else for (const needle of calls) if (!bundle.includes(needle)) problems.push(`${entry}: the bundle does not call ${needle}.`);
    if (!problems.length) console.log(`Card bundle OK: ${entry} (${meta.config.location}), ${(bundle.length / 1024).toFixed(1)} KB`);
  }
} finally {
  rmSync(temp, { recursive: true, force: true });
}
if (problems.length) {
  console.error(problems.join('\n'));
  process.exit(1);
}
