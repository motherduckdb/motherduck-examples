-- Editorial: newsroom conventions — tinted stock, red marker, serif headline
-- that states the finding, sans dek, source line.
--
-- Rendered example: ../assets/editorial.png
--
-- Edit the palette and the title, run it to create the Guide privately, then
-- promote it with MD_SET_GUIDE_ACCESS once a Dive comes back the way you want.

SELECT id, topic, title, access, current_version
FROM MD_CREATE_GUIDE(
  topic = 'dives',
  title = 'Editorial Dive style',
  description = 'Dive styling for charts that read like a news graphic: tinted paper, red marker rule, serif headline stating the finding, sans dek with units, bars with the scale on the right, source line',
  content = '
# Editorial

These rules replace the corresponding MotherDuck defaults. Anything not listed
here follows the built-in design guidance.

## Color

- Page background: #fff1e5
- Text: #33302e
- Muted text and labels: #66605c
- Hairline rules: #cec6b5
- Accent, one only: #e3120b

Bars, KPI figures, and the marker rule all use the accent. No second hue.

## Typography

- Headline: `font-serif text-3xl font-bold leading-tight`.
- Everything else in `font-sans`: dek `text-sm` muted, section titles
  `text-sm font-semibold`, labels `text-xs`.
- KPI figures: `text-3xl font-semibold tabular-nums` in the accent color.
- Recharts ticks: `tick={{ fontSize: 10, fill: "#66605c" }}`.

## Layout

- A 44 x 5 px accent block above the headline. It is the page marker; nothing
  else uses that shape.
- Headline, then dek, then a hairline, then four KPIs in
  `grid grid-cols-4 gap-6`.
- A source line at the bottom, `text-xs` muted above a hairline:
  "Source: <fully qualified table>". Every page carries one.

## Number and date formatting

- Currency in millions to one decimal: $71.8M
- Percentages to one decimal: 18.8%
- Counts in millions to two decimals: 3.25M
- The dek carries place, period, and unit: "New York City yellow taxi revenue,
  November 2022, USD"

## Charts

- Bars, solid accent fill, roughly 170 px tall. Use a line only for a series
  long enough that bars would not resolve.
- Horizontal gridlines only (`vertical={false}`) in the hairline color.
- Put the y scale on the right (`orientation="right"`). The x axis line is
  1 px in the text color.
- The unit goes in a `text-xs` muted sub-label under the chart title, not on
  the axis.

## Tables

- Three rows. A table here supports the headline; browsing belongs elsewhere.

## Copy

- The headline states the finding and names the comparison it rests on:
  "Thanksgiving costs taxis a third of a Thursday", not "Revenue dips late in
  the month" and not "Daily revenue".
- Only claim what the query shows. Never an adjective without a number behind
  it.
- Chart and table titles stay descriptive; the finding lives in the headline.
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
