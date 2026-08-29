# Historical baseline fixture

`historical_baseline_monthly.csv` is a deterministic development fixture, not
an authoritative climate-normal dataset. Its values are plausible illustrative
monthly means intended to exercise transformation, anomaly, and reporting code
while the pipeline is being built.

Assumptions:

- The fixture contains exactly one row for each configured region and calendar
  month: 5 regions × 12 months = 60 source rows.
- Temperatures are monthly mean degrees Celsius, humidity values are monthly
  mean relative humidity percentages, and precipitation values are mean daily
  millimetres within the month.
- The seed command expands a monthly value to every valid day in that month
  using a leap-year calendar. February 29 therefore receives February's value,
  producing 366 baseline rows per region and 1,830 rows in total.
- Every day within a month intentionally has the same value. The fixture does
  not represent day-to-day variability, uncertainty, long-term trends, or an
  official climatological reference period.
- Region identifiers must exactly match `config/regions.yaml`. Changes to the
  checked-in fixture are applied by upsert; unchanged rows retain `updated_at`.

Replace this fixture with a cited authoritative dataset before using anomaly
results for scientific, operational, financial, or safety decisions.
