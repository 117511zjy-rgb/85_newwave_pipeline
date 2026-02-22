## Quickstart (reproduce main outputs)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# main figures/tables + paper bundle
python -u scripts/rupture_score.py
python -u scripts/run_paper_gen.py
