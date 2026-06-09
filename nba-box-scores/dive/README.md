# nba-box-scores — dive (frontend)

The consolidated NBA Box Scores **Dive** that replaces the legacy Next.js/Vercel
frontend. Reads `nba_box_scores_v3` directly via `useSQLQuery`. The companion
ingest pipeline lives in [`../flight`](../flight).

Three tabs, sharing one season/type/team/player filter bar:
- **Schedule** — games grouped by date → click-through box-score modal → per-player game log.
- **Game Quality** — player Game Quality leaderboard (drillable to game log).
- **Trends** — points/game vs. avg Game Quality scatter.

## Layout

```
nba-box-scores/dive/
├── src/            # the dive: dive.tsx (default export), components/, lib/
├── bundle.mjs      # esbuild → one self-contained dist/dive.jsx
├── package.json    # build only (esbuild)
└── dist/           # build output (git-ignored)
```

The local Vite preview harness (`.dive-preview/`) is **git-ignored** — it's a
throwaway dev scaffold (see the MotherDuck dives guide to regenerate it). The
source of record is `src/`; the deployed artifact is produced by `bundle.mjs`.

## Build

```bash
cd nba-box-scores/dive
npm install      # esbuild
npm run build    # → dist/dive.jsx
```

`bundle.mjs` inlines the local `src/` imports and keeps the runtime-provided
libraries external (`react`, `react-dom`, `recharts`, `d3`, `lucide-react`,
`@motherduck/react-sql-query`).

## Deploy

The dive is owned by `matson` on prod
(id `d474d326-4b1d-4caa-844f-f6eb1bdc52e9`). Deploy/update the content via the
MotherDuck SQL functions, reading the bundle from disk — e.g. with the DuckDB
CLI (use a **1.5.2** client; MotherDuck rejects 1.5.3):

```sql
SET VARIABLE c = (SELECT content FROM read_text('dist/dive.jsx'));
FROM MD_UPDATE_DIVE_CONTENT(
  id := 'd474d326-4b1d-4caa-844f-f6eb1bdc52e9'::UUID,
  content := getvariable('c'),
  required_resources := [{'url': 'md:nba_box_scores_v3', 'alias': 'nba_box_scores_v3'}],
  api_version := 1
);
```

(First-time creation uses `MD_CREATE_DIVE` with the same args plus `title`.)
The MCP `save_dive` / `update_dive` tools also work but require pasting the full
content as a parameter.

## Data notes

- Aggregate by stable `entity_id`, not `player_name` — names drift across
  seasons (e.g. "Nikola Jokić" / "Nikola Jokic"). Show the latest via
  `arg_max(player_name, game_date)`.
- `game_quality = -1` is the sub-15-min sentinel; exclude it (`>= 0`) from GQ
  averages.
- The MotherDuck dive renderer ignores Tailwind responsive prefixes (`md:` etc.)
  and arbitrary bracket values (`z-[60]`); use inline `style` for custom values.
