// The card's packages are installed here (test/node_modules), not next to the card source,
// so point the card's imports at them.
import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

const here = (p) => fileURLToPath(new URL(p, import.meta.url));

export default defineConfig({
  resolve: {
    alias: [
      { find: /^@hubspot\/ui-extensions$/, replacement: here('./node_modules/@hubspot/ui-extensions') },
      // The cards import HubSpot's CRM components from this subpath, which they would not find from their own folder.
      { find: /^@hubspot\/ui-extensions\/crm$/, replacement: here('./node_modules/@hubspot/ui-extensions/dist/crm/index.js') },
      { find: /^react(\/.*)?$/, replacement: here('./node_modules/react') + '$1' },
    ],
  },
  oxc: { jsx: { runtime: 'automatic' } },
  test: {
    include: ['*.test.{ts,tsx,mjs}'],
    environment: 'node',
  },
});
