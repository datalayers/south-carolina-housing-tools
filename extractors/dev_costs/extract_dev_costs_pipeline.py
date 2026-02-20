import os
import re
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np


# ---------- CONSTANTS ----------

OUTPUT_BASENAME = "SC_TEB_Development_Costs"

PDF_NAME_KEYWORDS = ["application", "teb"]

PAGE_ANCHORS_ALL = [
    "development costs",
    "total development cost",
    "total eligible basis",
]

NUM_PAT = r"\d{1,3}(?:,\d{3})+|\d+"
DASH_PAT = r"(?:(?<=\s)-(?=\s)|^-$)"


# ---------- CLI ----------

def parse_args():
    project_root = Path(__file__).resolve().parents[2]

    default_output = project_root / "outputs" / "dev_costs"

    p = argparse.ArgumentParser(description="Extract SC TEB Development Costs tables.")
    p.add_argument("--root", required=True, help="Root directory containing deal folders.")
    p.add_argument("--output-dir", default=str(default_output), help="Output directory.")
    return p.parse_args()


# ---------- HELPERS ----------

def find_candidate_pdfs(deal_dir: Path) -> list[Path]:
    pdfs = []
    for root, _, files in os.walk(deal_dir):
        for fn in files:
            if fn.lower().endswith(".pdf"):
                pdfs.append(Path(root) / fn)

    if not pdfs:
        return []

    def score(p: Path):
        name = p.name.lower()
        s = 0
        if "application" in name:
            s += 40
        if "workbook" in name:
            s += 25
        if "teb" in name:
            s += 10
        if "executed" in name:
            s -= 30
        try:
            s += min(25, int(p.stat().st_size / 1_000_000))
        except:
            pass
        return s

    return sorted(pdfs, key=score, reverse=True)


def extract_text_pdfplumber(pdf_path: Path):
    import pdfplumber
    lines = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages):
            txt = page.extract_text() or ""
            txt = txt.replace("\u00a0", " ")
            page_lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
            lines.append((i, page_lines))
    return lines


def page_has_anchors(page_lines):
    blob = " ".join(page_lines).lower()
    return all(a in blob for a in PAGE_ANCHORS_ALL)


def parse_development_cost_lines(page_lines):
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

        def to_num(tok):
            if tok == "-" or tok is None or tok == "":
                return np.nan
            return float(tok.replace(",", ""))

        nums = [to_num(t) for t in tokens]

        dev = basis_acq = basis_newrehab = summary_addm = diff = np.nan

        if len(tokens) == 1:
            dev = nums[0]
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

    df["Eligible Basis (Acq + New/Rehab)"] = (
        df["4% Basis (Acquisition)"].fillna(0)
        + df["4%/9% Basis (New/Rehab)"].fillna(0)
    )
    df.loc[df["Eligible Basis (Acq + New/Rehab)"] == 0,
           "Eligible Basis (Acq + New/Rehab)"] = np.nan

    return df


# ---------- MAIN ----------

def run_pipeline():
    args = parse_args()

    root = Path(args.root)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    deal_dirs = [d for d in root.iterdir() if d.is_dir()]

    status_rows = []
    master_rows = []

    for deal_dir in deal_dirs:
        deal_id = deal_dir.name
        candidates = find_candidate_pdfs(deal_dir)

        if not candidates:
            status_rows.append({"Deal Folder": str(deal_dir),
                                "Status": "NO_PDF_FOUND"})
            continue

        extracted = False

        for pdf_path in candidates:
            pages = extract_text_pdfplumber(pdf_path)

            for idx, lines in pages:
                if page_has_anchors(lines):
                    df = parse_development_cost_lines(lines)

                    if df.empty:
                        continue

                    df["Deal Folder"] = str(deal_dir)
                    df["PDF Used"] = str(pdf_path)
                    df["DevCost Page (0-index)"] = idx

                    master_rows.append(df)

                    status_rows.append(
                        {"Deal Folder": str(deal_dir),
                         "Status": "OK",
                         "PDF Used": str(pdf_path)}
                    )

                    extracted = True
                    break

            if extracted:
                break

        if not extracted:
            status_rows.append(
                {"Deal Folder": str(deal_dir),
                 "Status": "NO_DEV_COST_PAGE_FOUND"}
            )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_master_csv = output_dir / f"{OUTPUT_BASENAME}_master_{ts}.csv"
    out_status_csv = output_dir / f"{OUTPUT_BASENAME}_status_{ts}.csv"

    status_df = pd.DataFrame(status_rows)
    status_df.to_csv(out_status_csv, index=False)

    if master_rows:
        master_df = pd.concat(master_rows, ignore_index=True)
        master_df.to_csv(out_master_csv, index=False)

    print("\nDONE")
    print("Status:", out_status_csv)
    print("Master:", out_master_csv)


if __name__ == "__main__":
    run_pipeline()