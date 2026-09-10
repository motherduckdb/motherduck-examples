-- Terminal: dark operational console, monospace, green ok and amber warn.
--
-- Rendered example: ../assets/terminal.png
--
-- Edit the palette and the title, run it to create the Guide privately, then
-- promote it with MD_SET_GUIDE_ACCESS once a Dive comes back the way you want.

SELECT id, topic, title, access, current_version
FROM MD_CREATE_GUIDE(
  topic = 'dives',
  title = 'Terminal Dive style',
  description = 'Dive styling for dark operational dashboards: near-black background, monospace throughout, bordered panels, bracketed snake_case labels, green values with amber warnings, status line',
  content = '
# Terminal

These rules replace the corresponding MotherDuck defaults. Anything not listed
here follows the built-in design guidance. Use this style for dashboards people
watch during an incident, not for anything a customer reads.

## Color

- Page background: #0d1117
- Panel background: #161b22
- Borders and gridlines: #21262d
- Text: #c9d1d9
- Dim text and labels: #8b949e
- Values, nominal: #3fb950
- Warnings and anomalies: #d29922

Never go dimmer than #8b949e on this background — anything paler fails contrast
at small sizes.

## Typography

- Everything in `font-mono`, including axis ticks:
  `tick={{ fontSize: 10, fill: "#8b949e", fontFamily: "monospace" }}`.
- Section headers: `text-xs uppercase` in brackets, `[ revenue_per_day ]`.
- Labels are snake_case identifiers, not prose: `avg_fare`, not "Average fare".
- KPI values `text-2xl tabular-nums` in the nominal color.

## Layout

- This is the one style that wants chrome: KPIs and charts each sit in a panel
  with `background: #161b22` and a 1 px #21262d border.
- Four KPIs in `grid grid-cols-4 gap-3`, label above value.
- A status line closes the page, `text-xs` dim: source table, row count, and
  state — "src sample_data.nyc.taxi · rows 3,252,620 · ok".

## Number and date formatting

- Currency in millions to two decimals: $71.79M
- Percentages to one decimal: 18.8%
- Counts with thousands separators in the status line, in millions to two
  decimals in a KPI
- Dates as ISO: 2022-11-01, and ranges as "2022-11-01 → 2022-11-30"
- Every numeric column right-aligned and `tabular-nums`

## Charts

- One 1.5 px line in the nominal color, `dot={false}`, roughly 170 px tall.
- Dotted grid: `strokeDasharray="1 3"` in #21262d. Axis lines in the same color.
- Call out the extreme under the chart in the warning color, prefixed with `!`:
  "! min 11-24 $1.44M". One line, and only when a value is worth acting on.

## Tables

- Five rows. Row borders in #21262d, no fills, no striping.
- Category values lowercased and snake_cased to match the labels.

## Copy

- No sentences. Identifiers, values, units.
- Titles state the measure. Anything resembling interpretation goes in the `!`
  line or nowhere.
',
  access = 'user'
);

-- Publish org-wide once a Dive comes back the way you want. Needs admin
-- permission; promotion does not change the Guide owner.
--
--   SELECT access
--   FROM MD_SET_GUIDE_ACCESS(
--     id = '<guide_id_returned_above>',
--     access = 'organization'
--   );
