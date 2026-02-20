\# South Carolina Housing Tools



A growing set of pipelines and extractors for South Carolina housing finance applications (TEB / 4% LIHTC, etc.).



\## Structure



\- `pipelines/` — download + ingest data (ZIPs, PDFs)

\- `extractors/` — parse downloaded documents into structured data (CSV)

\- `shared/` — reusable helpers (PDF utilities, logging, etc.)

\- `outputs/`, `logs/` — generated artifacts (gitignored)



\## Quick start



Install dependencies:



```bash

pip install -r requirements.txt





Download 2025 TEB applications:



py -3.14 .\\pipelines\\teb\_4pct\\download\_applications.py --years 2025



Extract coordinates:



py -3.14 .\\extractors\\teb\_4pct\_coordinates\\extract\_coords.py --root "I:\\My Drive\\2025 South Carolina TEB"





\## 5B) Minimal module READMEs (optional but nice)

You can skip for now, but if you want:

```powershell

notepad .\\pipelines\\teb\_4pct\\README.md

notepad .\\extractors\\teb\_4pct\_coordinates\\README.md

