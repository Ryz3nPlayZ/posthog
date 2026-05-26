# hog-charts

PostHog's canvas-based charting library, used for trends, dashboards, and any
in-app chart that needs to render thousands of points smoothly. D3 powers the
scales; the canvas does the drawing; React handles overlays.

```tsx
import { LineChart } from 'lib/hog-charts'
import type { ChartTheme, Series } from 'lib/hog-charts'

const SERIES: Series[] = [{ key: 'a', label: 'A', data: [10, 20, 30] }]
const LABELS = ['Mon', 'Tue', 'Wed']
const THEME: ChartTheme = { colors: ['#1f77b4'], backgroundColor: '#ffffff' }

;<LineChart series={SERIES} labels={LABELS} theme={THEME} />
```

## Series

- `series.overlay` (default `false`): marks an auxiliary series derived from
  primary data — trend lines and moving averages. Excluded from stack
  computation and from the y-axis baseline calculation, so a trendline
  projection won't drag the axis below 0 when the underlying data is
  non-negative. (CI bands are not overlays — they represent real data
  uncertainty whose range should still influence the axis.)

`series.visibility` controls where a series appears:

- `excluded` (default `false`): fully excludes the series — no rendering, no
  scale contribution, no tooltip row, no hit-testing.
- `tooltip` (default `true`): when `false`, the series still renders and
  participates in scales and hit-testing, but is omitted from
  `TooltipContext.seriesData` so it doesn't appear as a tooltip row.
- `valueLabel` (default `true`): when `false`, the `ValueLabels` overlay
  skips this series.

## Custom tooltip

Pass a render prop to `tooltip`. It receives `TooltipContext` — `seriesData`,
`label`, `dataIndex`, `position`, etc. Omit to use the built-in
`DefaultTooltip`.

```tsx
<LineChart
  series={SERIES}
  labels={LABELS}
  theme={THEME}
  tooltip={(ctx) => <MyTooltip label={ctx.label} rows={ctx.seriesData} />}
/>
```

## Custom overlays

Render any React component as a child of the chart and read layout / hover
state through hooks:

- `useChartLayout()` — scales, dimensions, theme, resolved values. Doesn't
  re-render on hover.
- `useChartHover()` — hovered data point. Re-renders on every mousemove.
- `useChart()` — both, kept for back-compat. Use the granular hooks above
  unless you genuinely need both shapes.

```tsx
function GoalLine() {
  const { scales } = useChartLayout()
  const y = scales.y(100)
  return <div style={{ position: 'absolute', top: y, left: 0, right: 0, borderTop: '1px dashed' }} />
}

;<LineChart series={SERIES} labels={LABELS} theme={THEME}>
  <GoalLine />
</LineChart>
```

## Sparkline and Metric

Two layers, both exported from `lib/hog-charts`.

- `Sparkline` — axis-less line+area preset over `LineChart`. Exposes
  `onHoverIndexChange` so consumers can drive a hover-following headline
  without subscribing to `useChartHover` directly.
- `Metric` — dashboard tile: title, big value, optional comparison pill,
  subtitle, and an embedded sparkline when `data` is passed. Owns the hover
  state and the headline animation. Omit `data` for a number-only tile.

```tsx
import { Metric } from 'lib/hog-charts'
;<Metric
  title="Total Revenue"
  data={[4200, 5100, 4700, /* … */ 8800]}
  labels={['Jan', 'Feb', 'Mar', /* … */ 'Dec']}
  theme={THEME}
  formatValue={(v) => `US$${Math.round(v).toLocaleString()}`}
/>
```

When `data` is present, the change pill defaults to comparing the current
point to the first non-zero value in the series. Pass `showChange={false}` to
hide it, `formatChange` to customize the percentage label, or
`change={{ value, label }}` to supply a fixed comparison that doesn't update
on hover. For a number-only tile, omit `data` and pass `value` directly.

## More

- Building a new chart type, library architecture, conventions →
  [docs/CONTRIBUTING.md](./docs/CONTRIBUTING.md)
- Writing tests against a chart, or against code that uses one →
  [docs/TESTING.md](./docs/TESTING.md)
