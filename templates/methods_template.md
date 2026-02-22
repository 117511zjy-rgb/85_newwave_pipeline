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
