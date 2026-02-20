import os
import re
import sys
import math
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

# ---------- CONFIG ----------
ROOT_DIR = r"I:\My Drive\2025 South Carolina TEB"   # <-- change this
OUTPUT_DIR = r"I:\My Drive\2025 South Carolina TEB\_outputs"  # <-- change this
OUTPUT_BASENAME = "SC_TEB_Development_Costs"

# How to identify an "application" PDF
PDF_NAME_KEYWORDS = ["application", "teb"]
FALLBACK_SCAN_ALL_PDFS = True

# Page anchors for the Development Costs table
PAGE_ANCHORS_ALL = [
    "development costs",
    "total development cost",
    "total eligible basis",
]

# If True, try every PDF if none match keywords (slower but more robust)
FALLBACK_SCAN_ALL_PDFS = True

# OCR fallback (optional): set to True only if you already have pdf2image + poppler + pytesseract working
ENABLE_OCR_FALLBACK = False

# If ENABLE_OCR_FALLBACK = True, you must have these installed:
# pip install pdf2image pytesseract pillow
# and Poppler installed and on PATH
# ---------- END CONFIG ----------


NUM_PAT = r"\d{1,3}(?:,\d{3})+|\d+"
DASH_PAT = r"(?:(?<=\s)-(?=\s)|^-$)"


def score(p: Path) -> int:
    name = p.name.lower()
    s = 0

    # Strong preference for the full package
    if "teb application" in name:
        s += 100
    if re.search(r"\b\d{5}\b", name):  # often includes app #
        s += 20
    if "application executed" in name:
        s += 30

    # keywords
    for k in kw:
        if k in name:
            s += 10

    # prefer larger PDFs (usually the full application)
    try:
        s += int(p.stat().st_size / 1_000_000)  # +1 per MB
    except:
        pass

    # slight preference to shorter paths
    s -= len(p.parts)

    return s

    # prioritize keyword matches
    keyword_matches = [p for p in pdfs if any(k in p.name.lower() for k in kw)]
    if keyword_matches:
        return sorted(keyword_matches, key=score, reverse=True)

    return sorted(pdfs, key=score, reverse=True) if FALLBACK_SCAN_ALL_PDFS else []


def extract_text_pdfplumber(pdf_path: Path) -> list[str]:
    import pdfplumber
    lines = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages):
            txt = page.extract_text() or ""
            # normalize whitespace
            txt = txt.replace("\u00a0", " ")
            page_lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
            lines.append((i, page_lines))
    return lines


def page_has_anchors(page_lines: list[str]) -> bool:
    blob = " ".join(page_lines).lower()
    return all(a in blob for a in PAGE_ANCHORS_ALL)


def parse_development_cost_lines(page_lines: list[str]) -> pd.DataFrame:
    """
    Parse the numbered line items (1..80 etc.) into structured columns.
    Designed for SC Housing TEB Development Costs pages.
    """
    rows = []
    for raw in page_lines:
        s = raw.strip()
        m = re.match(r"^(\d{1,3})\s+(.*)$", s)
        if not m:
            continue

        line_no = int(m.group(1))
        rest = m.group(2)

        first_num = re.search(NUM_PAT, rest)
        if first_num:
            item = rest[: first_num.start()].strip()
            tail = rest[first_num.start():]
            tokens = re.findall(f"{NUM_PAT}|{DASH_PAT}", tail)
        else:
            item = rest.strip()
            tokens = []

        def to_num(tok: str) -> float:
            if tok == "-" or tok is None or tok == "":
                return np.nan
            return float(tok.replace(",", ""))

        nums = [to_num(t) for t in tokens]

        # Heuristics for column placement:
        # Dev Costs | Basis Acq | Basis New/Rehab | Summary Addm | Difference
        dev = basis_acq = basis_newrehab = summary_addm = diff = np.nan

        if len(tokens) == 1:
            dev = nums[0]
        elif len(tokens) == 2:
            # often: Dev + Summary (e.g. Developer Fee line)
            dev, summary_addm = nums[0], nums[1]
        elif len(tokens) == 3 and tokens[1] == "-":
            # Dev - BasisNewRehab
            dev, basis_newrehab = nums[0], nums[2]
        elif len(tokens) >= 3:
            dev = nums[0]
            basis_acq = nums[1] if len(nums) > 1 else np.nan
            basis_newrehab = nums[2] if len(nums) > 2 else np.nan
            summary_addm = nums[3] if len(nums) > 3 else np.nan
            diff = nums[4] if len(nums) > 4 else np.nan

        rows.append(
            {
                "Line": line_no,
                "Item": item,
                "Development Costs": dev,
                "4% Basis (Acquisition)": basis_acq,
                "4%/9% Basis (New/Rehab)": basis_newrehab,
                "Summary Const Cost Addm": summary_addm,
                "Difference": diff,
                "Raw": s,
            }
        )

    df = pd.DataFrame(rows).sort_values("Line").reset_index(drop=True)

    # Helpful derived column
    df["Eligible Basis (Acq + New/Rehab)"] = (
        df["4% Basis (Acquisition)"].fillna(0) + df["4%/9% Basis (New/Rehab)"].fillna(0)
    )
    df.loc[df["Eligible Basis (Acq + New/Rehab)"] == 0, "Eligible Basis (Acq + New/Rehab)"] = np.nan

    return df


def extract_deal_id_and_name(deal_dir: Path) -> tuple[str, str]:
    # Try to parse leading numeric id like "52501 - Palomino Estates"
    name = deal_dir.name.strip()
    m = re.match(r"^(\d+)\s*[-–]\s*(.+)$", name)
    if m:
        return m.group(1), m.group(2).strip()
    return "", name


def ensure_outdir():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

def find_candidate_pdfs(deal_dir: str) -> list[str]:
    """
    Return a ranked list of PDF candidates within a deal folder.

    Heuristics:
      - search recursively for PDFs
      - prioritize likely full application/workbook PDFs
      - deprioritize executed versions and single-page attachments
    """
    pdfs = []
    for root, _, files in os.walk(deal_dir):
        for fn in files:
            if fn.lower().endswith(".pdf"):
                pdfs.append(os.path.join(root, fn))

    if not pdfs:
        return []

    def score(path: str) -> int:
        name = os.path.basename(path).lower()
        s = 0

        # Strong positives
        if "application" in name:
            s += 40
        if "workbook" in name:
            s += 25
        if "teb" in name:
            s += 10
        if "tab 1" in path.lower() or "tab 1" in name:
            s += 10

        # Negatives (common bad picks)
        if "executed" in name:
            s -= 30
        if "page 1" in name or "page1" in name:
            s -= 15
        if "development team" in name:
            s -= 10

        # Larger files are often "full app"
        try:
            s += min(25, int(os.path.getsize(path) / (1024 * 1024)))  # +1 per MB up to +25
        except Exception:
            pass

        return s

    pdfs.sort(key=score, reverse=True)
    return pdfs

def run_pipeline():
    ensure_outdir()

    root = Path(ROOT_DIR)
    deal_dirs = [Path(r"I:\My Drive\2025 South Carolina TEB\52501 - Palomino Estates")]

    status_rows = []
    master_rows = []

    for deal_dir in deal_dirs:
        deal_id, deal_name = extract_deal_id_and_name(deal_dir)
        candidates = find_candidate_pdfs(deal_dir)

        if not candidates:
            status_rows.append(
                {
                    "Deal Folder": str(deal_dir),
                    "Deal ID": deal_id,
                    "Deal Name": deal_name,
                    "Status": "NO_PDF_FOUND",
                    "PDF Used": "",
                    "DevCost Page": "",
                    "Notes": "No PDFs found in folder tree.",
                }
            )
            continue

        extracted = False
        last_err = ""

        for pdf_path in candidates:
            try:
                pages = extract_text_pdfplumber(pdf_path)

                # find dev costs page
                dev_page_idx = None
                dev_page_lines = None
                for (idx, lines) in pages:
                    if page_has_anchors(lines):
                        dev_page_idx = idx
                        dev_page_lines = lines
                        break

                if dev_page_idx is None:
                    continue

                df = parse_development_cost_lines(dev_page_lines)

                # sanity check: should contain line 77/78/79/80 usually
                if df.empty or (df["Line"].max() < 60):
                    continue

                df["Deal ID"] = deal_id
                df["Deal Name"] = deal_name
                df["Deal Folder"] = str(deal_dir)
                df["PDF Used"] = str(pdf_path)
                df["DevCost Page (0-index)"] = dev_page_idx

                master_rows.append(df)
                status_rows.append(
                    {
                        "Deal Folder": str(deal_dir),
                        "Deal ID": deal_id,
                        "Deal Name": deal_name,
                        "Status": "OK",
                        "PDF Used": str(pdf_path),
                        "DevCost Page": dev_page_idx,
                        "Notes": "",
                    }
                )
                extracted = True
                break

            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"
                continue

        if not extracted:
            status_rows.append(
                {
                    "Deal Folder": str(deal_dir),
                    "Deal ID": deal_id,
                    "Deal Name": deal_name,
                    "Status": "NO_DEV_COST_PAGE_FOUND",
                    "PDF Used": str(candidates[0]) if candidates else "",
                    "DevCost Page": "",
                    "Notes": last_err or "Could not find page anchors or parse table.",
                }
            )

    # Write outputs
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = Path(OUTPUT_DIR) / f"{OUTPUT_BASENAME}_{ts}.xlsx"
    out_status_csv = Path(OUTPUT_DIR) / f"{OUTPUT_BASENAME}_status_{ts}.csv"
    out_master_csv = Path(OUTPUT_DIR) / f"{OUTPUT_BASENAME}_master_{ts}.csv"

    status_df = pd.DataFrame(status_rows).sort_values(["Status", "Deal ID", "Deal Name"])
    status_df.to_csv(out_status_csv, index=False)

    if master_rows:
        master_df = pd.concat(master_rows, ignore_index=True)

        # A clean “screening-friendly” view
        clean_cols = [
            "Deal ID", "Deal Name", "Line", "Item",
            "Development Costs",
            "4% Basis (Acquisition)",
            "4%/9% Basis (New/Rehab)",
            "Eligible Basis (Acq + New/Rehab)",
            "Summary Const Cost Addm",
            "Difference",
            "PDF Used",
            "DevCost Page (0-index)",
        ]
        master_clean = master_df[clean_cols].copy()

        master_clean.to_csv(out_master_csv, index=False)

        # Excel with multiple sheets
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
            status_df.to_excel(writer, sheet_name="Status", index=False)
            master_clean.to_excel(writer, sheet_name="Master", index=False)

            # Optional: deal-level sheets (keeps workbook readable by limiting sheet name length)
            for (did, dname), g in master_clean.groupby(["Deal ID", "Deal Name"], dropna=False):
                sheet = (f"{did} {dname}" if did else dname)[:31]
                g2 = g.drop(columns=["Deal ID", "Deal Name", "PDF Used", "DevCost Page (0-index)"])
                g2.to_excel(writer, sheet_name=sheet, index=False)

        print(f"\nDONE")
        print(f"Excel:   {out_xlsx}")
        print(f"Status:  {out_status_csv}")
        print(f"Master:  {out_master_csv}")
    else:
        print("\nNo deals extracted successfully.")
        print(f"Status: {out_status_csv}")


if __name__ == "__main__":
    run_pipeline()