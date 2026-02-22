import os
import re
import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, List, Dict

import pandas as pd
import numpy as np


# ---------- CONSTANTS ----------

OUTPUT_BASENAME = "SC_TEB_Development_Costs"

PAGE_ANCHORS_ALL = [
    "development costs",
    "development cost",
    "development cost budget",
    "development budget",
    "sources and uses",
    "total development cost",
    "total development costs",
    "tdc",
    "eligible basis",
    "total eligible basis",
]

DASH_PAT = r"(?:(?<=\s)-(?=\s)|^-$)"


# ---------- CLI ----------


def parse_args():
    project_root = Path(__file__).resolve().parents[2]
    default_output = project_root / "outputs" / "dev_costs"
    default_logs = project_root / "logs" / "dev_costs"

    p = argparse.ArgumentParser(description="Extract SC TEB Development Costs tables.")
    p.add_argument(
        "--root", required=True, help="Root directory containing deal folders."
    )
    p.add_argument(
        "--output-dir", default=str(default_output), help="Output directory."
    )
    p.add_argument("--log-dir", default=str(default_logs), help="Log directory.")
    p.add_argument(
        "--excel", action="store_true", help="Write Excel workbook with all outputs."
    )
    p.add_argument(
        "--keep-raw-parsed",
        action="store_true",
        help="Keep intermediate/raw parsed rows.",
    )
    p.add_argument(
        "--max-pdfs-per-deal",
        type=int,
        default=5,
        help="Max PDFs to try per deal folder.",
    )
    p.add_argument(
        "--max-pages-per-pdf",
        type=int,
        default=50,
        help="Max pages to scan per PDF (text and OCR upper bound).",
    )

    # OCR knobs
    p.add_argument(
        "--ocr",
        action="store_true",
        help="Enable OCR fallback for scanned/image-only PDFs (slower).",
    )
    p.add_argument(
        "--ocr-max-pages",
        type=int,
        default=15,
        help="Max pages to OCR per scanned PDF before giving up and trying the next candidate PDF.",
    )
    p.add_argument(
        "--poppler-path",
        default="",
        help="Optional: Poppler bin path (folder containing pdftoppm).",
    )
    p.add_argument(
        "--tesseract-cmd",
        default="",
        help="Optional: Full path to tesseract.exe if not on PATH.",
    )
    p.add_argument(
        "--ocr-dpi",
        type=int,
        default=200,
        help="OCR DPI for pdf->image conversion (higher = slower, sometimes more accurate).",
    )
    return p.parse_args()


# ---------- LOGGING ----------


class Tee:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, s):
        for st in self.streams:
            try:
                st.write(s)
            except Exception:
                pass

    def flush(self):
        for st in self.streams:
            try:
                st.flush()
            except Exception:
                pass


# ---------- HELPERS ----------


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def slug_project_id(folder_name: str) -> str:
    m = re.match(r"^\s*(\d{5})\s*-\s*(.*)\s*$", folder_name.strip())
    if not m:
        return ""
    return m.group(1)


def project_name_from_folder(folder_name: str) -> str:
    m = re.match(r"^\s*(\d{5})\s*-\s*(.*)\s*$", folder_name.strip())
    if not m:
        return folder_name.strip()
    return m.group(2).strip()


def find_deal_folders(root: Path) -> List[Path]:
    deals = []
    if not root.exists():
        return deals

    for child in root.iterdir():
        if child.is_dir():
            if re.match(r"^\d{5}\s*-", child.name.strip()):
                deals.append(child)

    return sorted(deals, key=lambda p: p.name)


def find_candidate_pdfs(deal_dir: Path) -> List[Path]:
    pdfs: List[Path] = []
    for r, _, files in os.walk(deal_dir):
        for fn in files:
            if fn.lower().endswith(".pdf"):
                pdfs.append(Path(r) / fn)

    if not pdfs:
        return []

    def score(p: Path) -> int:
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
        except Exception:
            pass
        return s

    return sorted(pdfs, key=score, reverse=True)


def extract_text_pdfplumber(
    pdf_path: Path, max_pages: int
) -> List[Tuple[int, List[str]]]:
    import pdfplumber

    pages: List[Tuple[int, List[str]]] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        n = min(len(pdf.pages), max_pages)
        for i in range(n):
            page = pdf.pages[i]
            txt = page.extract_text() or ""
            txt = txt.replace("\u00a0", " ")
            page_lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
            pages.append((i, page_lines))
    return pages


def is_mostly_empty_pages(
    pages: List[Tuple[int, List[str]]], threshold: float = 0.90
) -> bool:
    if not pages:
        return True
    empty = sum(1 for _, lines in pages if not lines)
    return (empty / len(pages)) >= threshold


def ocr_page_lines(
    pdf_path: Path,
    page_num_1based: int,
    dpi: int = 200,
    poppler_path: Optional[str] = None,
    tesseract_cmd: Optional[str] = None,
) -> List[str]:
    """
    OCR a single page of a PDF (page_num_1based).
    Returns cleaned non-empty lines.
    """
    from pdf2image import convert_from_path
    import pytesseract

    if tesseract_cmd:
        pytesseract.pytesseract.tesseract_cmd = tesseract_cmd

    imgs = convert_from_path(
        str(pdf_path),
        dpi=dpi,
        first_page=page_num_1based,
        last_page=page_num_1based,
        poppler_path=poppler_path if poppler_path else None,
    )
    if not imgs:
        return []

    txt = pytesseract.image_to_string(imgs[0]) or ""
    txt = txt.replace("\u00a0", " ")
    return [ln.strip() for ln in txt.splitlines() if ln.strip()]


def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def has_any_anchor(lines: List[str]) -> bool:
    blob = " ".join([ln.lower() for ln in lines])
    return any(a in blob for a in PAGE_ANCHORS_ALL)


def parse_money(token: str) -> Optional[float]:
    if token is None:
        return None
    t = str(token).strip()
    if not t:
        return None

    if re.fullmatch(DASH_PAT, t):
        return None

    neg = False
    if t.startswith("(") and t.endswith(")"):
        neg = True
        t = t[1:-1].strip()

    t = t.replace("$", "").replace(",", "")
    t = re.sub(r"[^\d\.]", "", t)

    if not t:
        return None
    try:
        v = float(t)
        return -v if neg else v
    except Exception:
        return None


def extract_numbers_from_line(line: str) -> List[float]:
    raw = re.findall(
        r"\(?\$?\d{1,3}(?:,\d{3})+(?:\.\d+)?\)?|\(?\$?\d+(?:\.\d+)?\)?",
        line,
    )
    vals: List[float] = []
    for r in raw:
        v = parse_money(r)
        if v is not None:
            vals.append(v)
    return vals


def split_item_and_numbers(line: str) -> Tuple[str, List[float]]:
    vals = extract_numbers_from_line(line)
    item = line
    if vals:
        item = re.sub(r"\(?\$?\d{1,3}(?:,\d{3})+(?:\.\d+)?\)?", " ", item)
        item = re.sub(r"\(?\$?\d+(?:\.\d+)?\)?", " ", item)
        item = normalize_text(item)
    return item, vals


def clean_item(s: str) -> str:
    s = normalize_text(s)
    s = re.sub(r"^[\.\-\–\—\:]+", "", s).strip()
    s = re.sub(r"\s{2,}", " ", s).strip()
    return s


def categorize_item(item_clean: str) -> str:
    t = (item_clean or "").lower()

    if t.startswith("total ") or t in {"total", "column totals"}:
        return "Other"

    if any(
        k in t
        for k in ["land", "acquisition", "purchase", "closing cost", "title", "survey"]
    ):
        return "Acquisition"
    if any(
        k in t
        for k in [
            "new construction",
            "construction",
            "site work",
            "sitework",
            "on-site",
            "off-site",
            "improvements",
            "hard cost",
            "contractor",
            "general requirements",
        ]
    ):
        return "Hard Costs"
    if any(
        k in t
        for k in [
            "architect",
            "engineering",
            "legal",
            "appraisal",
            "market study",
            "phase i",
            "phase ii",
            "environmental",
            "permit",
            "impact fee",
            "soft cost",
            "consultant",
            "developer overhead",
        ]
    ):
        return "Soft Costs"
    if any(
        k in t
        for k in [
            "interest",
            "financing",
            "loan",
            "lender",
            "bond",
            "issuance",
            "credit enhancement",
            "placement",
            "underwriter",
            "fees - financing",
        ]
    ):
        return "Financing"
    if any(
        k in t
        for k in [
            "developer fee",
            "dev fee",
            "consulting fee (developer)",
            "development fee",
        ]
    ):
        return "Developer Fee"
    if any(
        k in t
        for k in [
            "reserve",
            "operating reserve",
            "replacement reserve",
            "capital reserve",
            "lease-up reserve",
        ]
    ):
        return "Reserves"

    return "Other"


def is_total_like(item_u: pd.Series) -> pd.Series:
    return (
        item_u.eq("COLUMN TOTALS")
        | item_u.eq("TOTAL DEVELOPMENT COST")
        | item_u.eq("TOTAL ELIGIBLE BASIS")
        | item_u.eq("TOTAL INELIGIBLE COSTS")
        | item_u.str.contains(r"^TOTAL\b", regex=True, na=False)
    )


# ---------- PARSING ----------


def extract_dev_cost_rows_from_page_lines(
    page_lines: List[str],
    project_folder: str,
    project_id: str,
    project_name: str,
    pdf_path: str,
    page_index: int,
) -> List[Dict]:
    rows: List[Dict] = []
    for ln_i, line in enumerate(page_lines, start=1):
        item, nums = split_item_and_numbers(line)

        if not nums:
            continue

        item_c = clean_item(item)
        if not item_c:
            continue

        parse_mode = ""
        dev_cost = None
        eligible_basis = None

        if len(nums) == 1:
            dev_cost = nums[0]
            eligible_basis = None
            parse_mode = "1col_dev_cost"
        elif len(nums) == 2:
            dev_cost = nums[0]
            eligible_basis = nums[1]
            parse_mode = "2col_dev_cost__eligible_basis_total"
        elif len(nums) == 3:
            dev_cost = nums[0]
            eligible_basis = nums[1]
            parse_mode = "3col_mapped"
        else:
            dev_cost = nums[0]
            eligible_basis = nums[1] if len(nums) > 1 else None
            parse_mode = "4plus_extra"

        rows.append(
            {
                "project_folder": project_folder,
                "project_id": project_id,
                "project_name": project_name,
                "pdf_path": pdf_path,
                "devcost_page": page_index + 1,
                "line": ln_i,
                "item_raw": line,
                "item_clean": item_c,
                "development_costs": dev_cost,
                "eligible_basis_total": eligible_basis,
                "num_values_found": len(nums),
                "parse_mode": parse_mode,
            }
        )

    return rows


def find_and_parse_devcost_page(
    pdf_path: Path,
    max_pages: int,
    deal_meta: Dict,
    enable_ocr: bool = False,
    ocr_dpi: int = 200,
    ocr_max_pages: int = 15,
    poppler_path: str = "",
    tesseract_cmd: str = "",
) -> Tuple[str, Optional[int], List[Dict], str]:
    """
    Returns:
      status, page_index (0-based), rows, notes

    Strategy:
      1) Try pdfplumber text extraction over pages.
      2) If scanned (mostly empty) AND OCR enabled:
         OCR pages one-by-one, check anchors, stop immediately when found.
         Cap OCR work per scanned PDF via ocr_max_pages.
    """
    # --- Pass 1: text extraction (fast) ---
    pages = extract_text_pdfplumber(pdf_path, max_pages=max_pages)

    # Try to find anchor in extracted text first
    for page_index, lines in pages:
        if has_any_anchor(lines):
            rows = extract_dev_cost_rows_from_page_lines(
                page_lines=lines,
                project_folder=deal_meta["project_folder"],
                project_id=deal_meta["project_id"],
                project_name=deal_meta["project_name"],
                pdf_path=str(pdf_path),
                page_index=page_index,
            )
            if rows:
                return "OK", page_index, rows, "anchor_found"
            return "NO_TABLE_ROWS_FOUND", page_index, [], "anchor_found_but_no_rows"

    # --- Pass 2: OCR early-stop (only if scanned + enabled) ---
    if enable_ocr and is_mostly_empty_pages(pages, threshold=0.90):
        ocr_pages = min(max_pages, max(1, int(ocr_max_pages)))
        print(
            f"[OCR] {pdf_path.name} looks scanned; running OCR (early-stop, cap={ocr_pages})...",
            flush=True,
        )

        # OCR sequentially; stop as soon as anchor found
        for page_num in range(1, ocr_pages + 1):  # 1-based for pdf2image
            print(f"[OCR] {pdf_path.name} page {page_num}/{ocr_pages}", flush=True)

            lines = ocr_page_lines(
                pdf_path=pdf_path,
                page_num_1based=page_num,
                dpi=ocr_dpi,
                poppler_path=poppler_path or None,
                tesseract_cmd=tesseract_cmd or None,
            )

            if not lines:
                continue

            if has_any_anchor(lines):
                page_index0 = page_num - 1
                rows = extract_dev_cost_rows_from_page_lines(
                    page_lines=lines,
                    project_folder=deal_meta["project_folder"],
                    project_id=deal_meta["project_id"],
                    project_name=deal_meta["project_name"],
                    pdf_path=str(pdf_path),
                    page_index=page_index0,
                )
                if rows:
                    return "OK", page_index0, rows, "anchor_found_ocr_early_stop"
                return (
                    "NO_TABLE_ROWS_FOUND",
                    page_index0,
                    [],
                    "anchor_found_but_no_rows_ocr_early_stop",
                )

        return "NO_DEV_COST_PAGE_FOUND", None, [], "no_anchor_found_ocr_early_stop"

    # Not scanned or OCR disabled
    return "NO_DEV_COST_PAGE_FOUND", None, [], "no_anchor_found"


# ---------- PHASE 2: SUMMARY + METRICS ----------


def build_deal_summary(line_items: pd.DataFrame) -> pd.DataFrame:
    if line_items is None or line_items.empty:
        return pd.DataFrame(
            columns=[
                "project_folder",
                "project_id",
                "project_name",
                "tdc_total",
                "eligible_basis_total",
                "num_line_items",
                "num_parse_issues",
                "tdc_source",
                "basis_source",
            ]
        )

    df = line_items.copy()
    df["item_clean_u"] = df["item_clean"].astype(str).str.strip().str.upper()

    gcols = ["project_folder", "project_id", "project_name"]
    out_rows: List[Dict] = []

    for (pf, pid, pname), g in df.groupby(gcols, dropna=False):
        tdc_row = g[g["item_clean_u"].eq("TOTAL DEVELOPMENT COST")]
        basis_row = g[g["item_clean_u"].eq("TOTAL ELIGIBLE BASIS")]
        non_total = g[~is_total_like(g["item_clean_u"])].copy()

        if not tdc_row.empty and pd.notna(tdc_row["development_costs"].iloc[0]):
            tdc_total = float(tdc_row["development_costs"].iloc[0])
            tdc_source = "TOTAL_DEVELOPMENT_COST_ROW"
        else:
            tdc_total = float(non_total["development_costs"].sum(skipna=True))
            tdc_total = np.nan if tdc_total == 0 else tdc_total
            tdc_source = "SUM_NON_TOTAL_ROWS_FALLBACK"

        if not basis_row.empty and pd.notna(basis_row["development_costs"].iloc[0]):
            eligible_total = float(basis_row["development_costs"].iloc[0])
            basis_source = "TOTAL_ELIGIBLE_BASIS_ROW"
        else:
            eligible_total = float(non_total["eligible_basis_total"].sum(skipna=True))
            eligible_total = np.nan if eligible_total == 0 else eligible_total
            basis_source = "SUM_ELIGIBLE_BASIS_FALLBACK"

        out_rows.append(
            {
                "project_folder": pf,
                "project_id": pid,
                "project_name": pname,
                "tdc_total": tdc_total,
                "eligible_basis_total": eligible_total,
                "num_line_items": int(non_total.shape[0]),
                "num_parse_issues": 0,
                "tdc_source": tdc_source,
                "basis_source": basis_source,
            }
        )

    return pd.DataFrame(out_rows)


def safe_div(a: float, b: float) -> float:
    try:
        if b is None or b == 0 or (isinstance(b, float) and np.isnan(b)):
            return np.nan
        return a / b
    except Exception:
        return np.nan


CATEGORY_ORDER = [
    "Acquisition",
    "Hard Costs",
    "Soft Costs",
    "Financing",
    "Developer Fee",
    "Reserves",
    "Other",
]


def build_deal_metrics(line_items: pd.DataFrame) -> pd.DataFrame:
    if line_items is None or line_items.empty:
        return pd.DataFrame()

    df = line_items.copy()
    df["item_clean_u"] = df["item_clean"].astype(str).str.strip().str.upper()

    base = build_deal_summary(df).copy()
    df_nt = df[~is_total_like(df["item_clean_u"])].copy()

    cat = (
        df_nt.groupby(
            ["project_folder", "project_id", "project_name", "item_category"],
            dropna=False,
        )
        .agg(cat_total=("development_costs", "sum"))
        .reset_index()
    )

    wide = cat.pivot_table(
        index=["project_folder", "project_id", "project_name"],
        columns="item_category",
        values="cat_total",
        aggfunc="sum",
    ).reset_index()

    for c in CATEGORY_ORDER:
        if c not in wide.columns:
            wide[c] = np.nan

    wide = wide.rename(
        columns={c: f"tdc_{c.lower().replace(' ', '_')}" for c in CATEGORY_ORDER}
    )

    m = base.merge(
        wide, on=["project_folder", "project_id", "project_name"], how="left"
    )

    m["basis_pct_of_tdc"] = m.apply(
        lambda r: safe_div(
            r.get("eligible_basis_total", np.nan), r.get("tdc_total", np.nan)
        ),
        axis=1,
    )

    for c in CATEGORY_ORDER:
        col = f"tdc_{c.lower().replace(' ', '_')}"
        m[f"{col}_pct"] = m.apply(
            lambda r: safe_div(r.get(col, np.nan), r.get("tdc_total", np.nan)), axis=1
        )

    m["flag_basis_gt_tdc"] = m["eligible_basis_total"] > m["tdc_total"]
    m["flag_missing_tdc"] = m["tdc_total"].isna()
    m["flag_missing_basis"] = m["eligible_basis_total"].isna()

    hc_pct = m.get("tdc_hard_costs_pct", pd.Series([np.nan] * len(m)))
    m["flag_hard_costs_low"] = hc_pct < 0.45
    m["flag_hard_costs_high"] = hc_pct > 0.85

    df_pct = m.get("tdc_developer_fee_pct", pd.Series([np.nan] * len(m)))
    m["flag_dev_fee_high"] = df_pct > 0.18

    def zscore(series: pd.Series) -> pd.Series:
        s = pd.to_numeric(series, errors="coerce")
        mu = s.mean(skipna=True)
        sd = s.std(skipna=True, ddof=0)
        if sd == 0 or np.isnan(sd):
            return pd.Series([np.nan] * len(s))
        return (s - mu) / sd

    m["z_tdc_total"] = zscore(m["tdc_total"])
    m["z_basis_pct_of_tdc"] = zscore(m["basis_pct_of_tdc"])
    m["flag_outlier_tdc"] = m["z_tdc_total"].abs() >= 2.5
    m["flag_outlier_basis_pct"] = m["z_basis_pct_of_tdc"].abs() >= 2.5

    return m


# ---------- QC REPORT ----------


def build_qc_report(deal_metrics: pd.DataFrame) -> pd.DataFrame:
    if deal_metrics is None or deal_metrics.empty:
        return pd.DataFrame()

    df = deal_metrics.copy()
    flag_cols = [c for c in df.columns if c.startswith("flag_")]
    if not flag_cols:
        return pd.DataFrame()

    for c in flag_cols:
        df[c] = df[c].fillna(False).astype(bool)

    df["flag_reasons"] = df.apply(
        lambda r: "; ".join(
            [c.replace("flag_", "") for c in flag_cols if r.get(c, False)]
        ),
        axis=1,
    )

    qc = df[df[flag_cols].any(axis=1)].copy()
    if qc.empty:
        return qc

    front = [
        "project_folder",
        "project_id",
        "project_name",
        "tdc_total",
        "eligible_basis_total",
        "basis_pct_of_tdc",
        "tdc_source",
        "basis_source",
        "flag_reasons",
    ]

    share_cols = [
        "tdc_hard_costs_pct",
        "tdc_soft_costs_pct",
        "tdc_financing_pct",
        "tdc_developer_fee_pct",
        "tdc_acquisition_pct",
        "tdc_reserves_pct",
        "tdc_other_pct",
    ]
    share_cols = [c for c in share_cols if c in qc.columns]

    rest_flags = [c for c in flag_cols if c in qc.columns]
    rest = [c for c in qc.columns if c not in (front + share_cols + rest_flags)]
    qc = qc[front + share_cols + rest_flags + rest]

    qc["flag_count"] = qc[flag_cols].sum(axis=1)
    qc = qc.sort_values(["flag_count", "tdc_total"], ascending=[False, False]).drop(
        columns=["flag_count"]
    )

    return qc


# ---------- PIPELINE ----------


def run_pipeline():
    args = parse_args()
    root = Path(args.root).expanduser()
    output_dir = Path(args.output_dir).expanduser()
    log_dir = Path(args.log_dir).expanduser()

    ensure_dir(output_dir)
    ensure_dir(log_dir)

    run_ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    ts = run_ts

    log_path = log_dir / f"run_dev_costs_{ts}.log"

    def logln(s: str = ""):
        print(s, flush=True)

    old_stdout = sys.stdout

    with open(log_path, "w", encoding="utf-8", errors="ignore") as lf:
        tee = Tee(old_stdout, lf)
        sys.stdout = tee
        try:
            logln(f"RUN_TS: {run_ts}")
            logln(f"ROOT: {root}")
            logln(f"OUTPUT_DIR: {output_dir}")
            logln(f"LOG_DIR: {log_dir}")
            logln(f"EXCEL: {args.excel}")
            logln(f"KEEP_RAW_PARSED: {args.keep_raw_parsed}")
            logln(f"MAX_PDFS_PER_DEAL: {args.max_pdfs_per_deal}")
            logln(f"MAX_PAGES_PER_PDF: {args.max_pages_per_pdf}")
            logln(f"OCR_ENABLED: {args.ocr}")
            if args.ocr:
                logln(f"OCR_DPI: {args.ocr_dpi}")
                logln(f"OCR_MAX_PAGES: {args.ocr_max_pages}")
                logln(f"POPPLER_PATH: {args.poppler_path}")
                logln(f"TESSERACT_CMD: {args.tesseract_cmd}")
            logln("")

            if not root.exists():
                raise FileNotFoundError(f'ROOT folder not found: "{root}"')

            deals = find_deal_folders(root)
            logln(f"DEALS_FOUND: {len(deals)}")
            logln("")

            status_csv = output_dir / f"{OUTPUT_BASENAME}_status_{ts}.csv"
            status_latest = output_dir / f"{OUTPUT_BASENAME}_status_latest.csv"

            misses_csv = output_dir / f"{OUTPUT_BASENAME}_misses_{ts}.csv"
            misses_latest = output_dir / f"{OUTPUT_BASENAME}_misses_latest.csv"

            line_items_csv = output_dir / f"{OUTPUT_BASENAME}_line_items_{ts}.csv"
            line_items_latest = output_dir / f"{OUTPUT_BASENAME}_line_items_latest.csv"

            deal_summary_csv = output_dir / f"{OUTPUT_BASENAME}_deal_summary_{ts}.csv"
            deal_summary_latest = (
                output_dir / f"{OUTPUT_BASENAME}_deal_summary_latest.csv"
            )

            deal_metrics_csv = output_dir / f"{OUTPUT_BASENAME}_deal_metrics_{ts}.csv"
            deal_metrics_latest = (
                output_dir / f"{OUTPUT_BASENAME}_deal_metrics_latest.csv"
            )

            qc_csv = output_dir / f"{OUTPUT_BASENAME}_QC_{ts}.csv"
            qc_latest = output_dir / f"{OUTPUT_BASENAME}_QC_latest.csv"

            excel_path = output_dir / f"{OUTPUT_BASENAME}_{ts}.xlsx"

            status_rows: List[Dict] = []
            parsed_rows: List[Dict] = []

            for deal_dir in deals:
                project_folder = deal_dir.name
                project_id = slug_project_id(project_folder)
                project_name = project_name_from_folder(project_folder)

                deal_meta = {
                    "project_folder": project_folder,
                    "project_id": project_id,
                    "project_name": project_name,
                }

                candidates = find_candidate_pdfs(deal_dir)[: args.max_pdfs_per_deal]
                if not candidates:
                    status_rows.append(
                        {
                            "project_folder": project_folder,
                            "project_id": project_id,
                            "project_name": project_name,
                            "status": "NO_PDF_FOUND",
                            "pdf_used": "",
                            "devcost_page": "",
                            "notes": "No PDFs found under deal folder.",
                        }
                    )
                    continue

                last_err = ""
                found_any = False

                for pdf_path in candidates:
                    logln(f"[SCAN] {project_folder} | {pdf_path.name}")

                    try:
                        status, page_index, rows, notes = find_and_parse_devcost_page(
                            pdf_path=pdf_path,
                            max_pages=args.max_pages_per_pdf,
                            deal_meta=deal_meta,
                            enable_ocr=args.ocr,
                            ocr_dpi=args.ocr_dpi,
                            ocr_max_pages=args.ocr_max_pages,
                            poppler_path=args.poppler_path,
                            tesseract_cmd=args.tesseract_cmd,
                        )

                        if status == "OK":
                            found_any = True
                            parsed_rows.extend(rows)
                            status_rows.append(
                                {
                                    "project_folder": project_folder,
                                    "project_id": project_id,
                                    "project_name": project_name,
                                    "status": "OK",
                                    "pdf_used": str(pdf_path),
                                    "devcost_page": (
                                        (page_index + 1)
                                        if page_index is not None
                                        else ""
                                    ),
                                    "notes": notes,
                                }
                            )
                            break
                        else:
                            last_err = status
                    except Exception as e:
                        last_err = f"EXCEPTION: {e}"

                if not found_any:
                    status_rows.append(
                        {
                            "project_folder": project_folder,
                            "project_id": project_id,
                            "project_name": project_name,
                            "status": "NO_DEV_COST_PAGE_FOUND",
                            "pdf_used": str(candidates[0]) if candidates else "",
                            "devcost_page": "",
                            "notes": last_err or "Could not find dev costs page.",
                        }
                    )

            status_df = pd.DataFrame(status_rows)
            status_df.to_csv(status_csv, index=False)
            status_df.to_csv(status_latest, index=False)

            misses_df = status_df[status_df["status"] != "OK"].copy()
            misses_df.to_csv(misses_csv, index=False)
            misses_df.to_csv(misses_latest, index=False)

            line_items_df = pd.DataFrame(parsed_rows)
            if not line_items_df.empty:
                for c in ["development_costs", "eligible_basis_total"]:
                    if c in line_items_df.columns:
                        line_items_df[c] = pd.to_numeric(
                            line_items_df[c], errors="coerce"
                        )

                line_items_df["item_category"] = (
                    line_items_df["item_clean"].astype(str).apply(categorize_item)
                )

                if "num_values_found" in line_items_df.columns:
                    line_items_df = line_items_df[
                        line_items_df["num_values_found"].fillna(0).astype(int) > 0
                    ].copy()

                if not args.keep_raw_parsed:
                    drop_cols = [c for c in ["item_raw"] if c in line_items_df.columns]
                    if drop_cols:
                        line_items_df = line_items_df.drop(columns=drop_cols)

            line_items_df.to_csv(line_items_csv, index=False)
            line_items_df.to_csv(line_items_latest, index=False)

            deal_summary_df = build_deal_summary(line_items_df)
            deal_metrics_df = build_deal_metrics(line_items_df)

            deal_summary_df.to_csv(deal_summary_csv, index=False)
            deal_summary_df.to_csv(deal_summary_latest, index=False)

            deal_metrics_df.to_csv(deal_metrics_csv, index=False)
            deal_metrics_df.to_csv(deal_metrics_latest, index=False)

            try:
                qc_df = build_qc_report(deal_metrics_df)
                qc_df.to_csv(qc_csv, index=False)
                qc_df.to_csv(qc_latest, index=False)
            except Exception as e:
                logln(f"QC generation failed: {e}")
                pd.DataFrame().to_csv(qc_csv, index=False)
                pd.DataFrame().to_csv(qc_latest, index=False)

            if args.excel:
                with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
                    status_df.to_excel(writer, sheet_name="status", index=False)
                    misses_df.to_excel(writer, sheet_name="misses", index=False)
                    line_items_df.to_excel(writer, sheet_name="line_items", index=False)
                    deal_summary_df.to_excel(
                        writer, sheet_name="deal_summary", index=False
                    )
                    deal_metrics_df.to_excel(
                        writer, sheet_name="deal_metrics", index=False
                    )
                    try:
                        qc_df.to_excel(writer, sheet_name="qc_report", index=False)
                    except Exception:
                        pass

            logln("")
            logln("DONE")
            logln(f"Status CSV:       {status_csv}")
            logln(f"Misses CSV:       {misses_csv}")
            logln(f"Line items CSV:   {line_items_csv}")
            logln(f"Deal summary CSV: {deal_summary_csv}")
            logln(f"Deal metrics CSV: {deal_metrics_csv}")
            if args.excel:
                logln(f"Excel:            {excel_path}")
            logln(f"Run log:          {log_path}")
            logln("")

            logln("STATUS COUNTS:")
            if not status_df.empty:
                logln(str(status_df["status"].value_counts()))
            else:
                logln("(empty)")
            logln("")

            if not line_items_df.empty and "parse_mode" in line_items_df.columns:
                logln("PARSE MODE COUNTS (CLEANED):")
                logln(str(line_items_df["parse_mode"].value_counts()))
                logln("")

            if not line_items_df.empty and "item_category" in line_items_df.columns:
                logln("CATEGORY COUNTS (CLEANED):")
                logln(str(line_items_df["item_category"].value_counts()))
                logln("")

        finally:
            sys.stdout = old_stdout


def main():
    run_pipeline()


if __name__ == "__main__":
    main()
