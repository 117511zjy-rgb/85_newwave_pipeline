# Paper Draft (auto-assembled)

## Methods

# Methods

## Data and pipeline
This study uses an event-level dataset (`data_raw/events_raw_4d_working.csv`) processed by a reproducible Python pipeline (`85_newwave_pipeline`). The pipeline outputs normalized 4D tables and figures into `outputs/`.

## 4D iconology features
We model a retrospective “4D iconology” with four axes:
- Time: longitudinal development trends derived from yearly aggregation.
- Space: distribution uniformity (e.g., concentration/inequality measures over cities).
- Society: institutional/policy logic captured by event-type and institution-level proxies.
- Technology: new variables in mediation and dissemination (platforms, digitization, archival interfaces).

## Rupture projection (O/R/S)
We project each event into a three-state rupture space:

- **O (0..1): Objectification / Archivalization strength**
  Computed from `archivalization_score_v2` (fallback: `archivalization_score`) normalized by /10 and clipped to [0,1], with small bonuses when geo metadata is informative (`geo_evidence`, `geo_scope`).

- **R (0..1): Rupture / polemic intensity**
  Computed from `polemic_score` normalized by /10, with an additive semantic-noise term:
  `R = clip(polemic01 + 0.45 * semantic_noise_01, 0, 1)`.

- **S (0..1): Structural opacity / missingness**
  A weighted index of missing/unknown fields (e.g., city, theme cluster, actors, organizations, geo scope/evidence).
  This is not treated as noise to be smoothed away, but as a constitutive opacity of the historical image-field.

## Yearly aggregation and visualization
For each year, we compute:
- Mean(O), Mean(R), Mean(S), and Mean(semantic_noise_01)
- Shares: O_share, R_share, S_share = each mean divided by (O+R+S)

Figures are exported as PNG and tables as CSV for direct inclusion.


## Results

# Results

## Outputs overview
This run exports:
- `fig_rupture_components_means.png`
- `fig_rupture_components_shares.png`
- `table_rupture_components_yearly.csv`

## Rupture components (means)
Insert and interpret the figure:
- O_mean indicates archivalization/objectification strength.
- R_mean indicates polemic/rupture intensity with semantic-noise contribution.
- S_mean indicates structural opacity / missingness.

## Rupture components (shares)
Insert and interpret the share figure:
- O_share / R_share / S_share show which mechanism dominates the rupture projection per year.

## Notes on retrospective 4D framing
We emphasize the dataset as a retrospective interface, not a complete reconstruction of on-site 1980s reality. The missingness and discontinuities are treated as part of the iconological field rather than purely technical gaps.


## Auto-generated summary (from rupture table)
- Years covered: 1979–2026 (n_years=16)
- Total event count used in yearly aggregation (sum of n): 69
- Peak O_mean: year 1986, O_mean=1.000
- Peak R_mean: year 2019, R_mean=0.476
- Peak S_mean: year 1994, S_mean=1.000


## Figures

- fig_rupture_components_means.png
- fig_rupture_components_shares.png
