import os
import re
import io
import sys
import argparse
from datetime import datetime

import fitz  # pip install PyMuPDF
import pandas as pd  # pip install pandas

from PIL import Image, ImageOps, ImageFilter  # pip install pillow
import pytesseract  # pip install pytesseract

# =============================
# REGEX
# =============================

COORD_ANCHOR_RE = re.compile(r"Coordinates\s+for\s+development\s+centroid", re.IGNORECASE)
DECIMAL_RE = re.compile(r"[-+]?\d{1,3}\.\d{4,6}")

# =============================
# LOGGING (print to console + log file)
# =============================

class Tee:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for s in self.streams:
            s.write(data)
            s.flush()

    def flush(self):
        for s in self.streams:
            s.flush()

# =============================
# PDF DISCOVERY / PICKER
# =============================

def find_pdfs_recursive(start_dir: str) -> list[str]:
    pdfs = []
    for root, _, files in os.walk(start_dir):
        for fn in files:
            if fn.lower().endswith(".pdf"):
                pdfs.append(os.path.join(root, fn))
    return pdfs


def score_pdf_for_coords(pdf_path: str, pages_to_check: int = 15) -> int:
    """
    Score PDFs by:
      - filename heuristics (avoid 'Application Page 1', avoid 'Executed', prefer 'TEB Application')
      - scanning up to 15 pages for anchor + nearby decimals
    """
    score = 0
    name = os.path.basename(pdf_path).lower()

    # filename heuristics
    if "executed" in name:
        score -= 25
    if "application page" in name:
        score -= 15  # often an attachment page, not the full application
    if "teb" in name:
        score += 5
    if name.strip() == "2025 teb application.pdf":
        score += 15
    if name.strip() == "2026 teb application.pdf":
        score += 15

    try:
        doc = fitz.open(pdf_path)
    except Exception:
        return -999

    try:
        for i in range(min(pages_to_check, len(doc))):
            txt = doc[i].get_text("text") or ""
            m = COORD_ANCHOR_RE.search(txt)
            if m:
                score += 20
                start = max(0, m.start() - 600)
                end = m.end() + 600
                window = txt[start:end]
                nums = DECIMAL_RE.findall(window)
                if len(nums) >= 2:
                    score += 50
                    break  # strong hit
    finally:
        doc.close()

    return score


def pick_best_application_pdf(tab1_path: str) -> str | None:
    pdfs = find_pdfs_recursive(tab1_path)
    if not pdfs:
        return None

    scored = [(score_pdf_for_coords(p), p) for p in pdfs]
    scored.sort(reverse=True, key=lambda x: x[0])

    best_score, best_path = scored[0]
    if best_score >= 10:
        return best_path

    # fallback: largest non-executed PDF
    non_executed = [p for p in pdfs if "executed" not in os.path.basename(p).lower()]
    if not non_executed:
        non_executed = pdfs
    non_executed.sort(key=lambda p: os.path.getsize(p) if os.path.exists(p) else 0, reverse=True)
    return non_executed[0] if non_executed else None

# =============================
# COORD NORMALIZATION
# =============================

def sc_bounds(lat: float, lon: float) -> bool:
    # South Carolina approximate bounds
    return (32.0 <= lat <= 35.6) and (-84.7 <= lon <= -78.0)


def normalize_sc(a: float, b: float) -> tuple[float, float] | None:
    """
    Decide lat/lon using abs values; enforce lon negative.
    """
    aa, bb = abs(a), abs(b)

    # a=lat, b=lon
    lat1, lon1 = aa, -bb
    if sc_bounds(lat1, lon1):
        return lat1, lon1

    # swapped
    lat2, lon2 = bb, -aa
    if sc_bounds(lat2, lon2):
        return lat2, lon2

    return None

# =============================
# TEXT EXTRACTION
# =============================

def extract_coords_from_text(txt: str) -> tuple[float | None, float | None]:
    m = COORD_ANCHOR_RE.search(txt)
    if not m:
        return None, None

    # window AROUND anchor (handles Excel-PDF text order weirdness)
    start = max(0, m.start() - 600)
    end = m.end() + 600
    window = txt[start:end]

    nums = DECIMAL_RE.findall(window)
    if len(nums) < 2:
        return None, None

    floats = []
    for n in nums:
        try:
            floats.append(float(n))
        except ValueError:
            pass

    for i in range(len(floats) - 1):
        norm = normalize_sc(floats[i], floats[i + 1])
        if norm:
            return norm[0], norm[1]

    return None, None

# =============================
# LIGHTWEIGHT OCR FALLBACK
# =============================

def render_page(page: fitz.Page, zoom: float = 4.5) -> Image.Image:
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    return Image.open(io.BytesIO(pix.tobytes("png")))


def preprocess(img: Image.Image, thresh: int = 175) -> Image.Image:
    """
    Contrast + sharpen + binarize. Helps with faint digits in yellow cells.
    """
    g = img.convert("L")
    g = ImageOps.autocontrast(g)
    g = g.filter(ImageFilter.SHARPEN)
    return g.point(lambda p: 255 if p > thresh else 0)


def ocr_image(img: Image.Image) -> str:
    config = "--oem 3 --psm 6 -c tessedit_char_whitelist=0123456789.-"
    txt = pytesseract.image_to_string(img, config=config)
    return (txt.replace("O", "0").replace("o", "0")
               .replace("I", "1").replace("l", "1").replace("|", "1"))


def parse_coords_from_ocr_text(txt: str) -> tuple[float | None, float | None]:
    nums = DECIMAL_RE.findall(txt)
    if len(nums) < 2:
        return None, None

    floats = []
    for n in nums:
        try:
            floats.append(float(n))
        except ValueError:
            pass

    for i in range(len(floats) - 1):
        norm = normalize_sc(floats[i], floats[i + 1])
        if norm:
            return norm[0], norm[1]

    return None, None


def ocr_coords_general(page: fitz.Page) -> tuple[float | None, float | None]:
    """
    Fast OCR attempt: one crop, no rotations, no brute force.
    """
    img = render_page(page, zoom=4.5)
    w, h = img.size

    # Generous top-right / site section region
    crop = img.crop((
        int(0.30 * w),  # left
        int(0.10 * h),  # top
        int(0.99 * w),  # right
        int(0.45 * h),  # bottom
    ))

    bw = preprocess(crop, thresh=175)
    txt = ocr_image(bw)
    return parse_coords_from_ocr_text(txt)

# =============================
# DOC EXTRACTION
# =============================

def extract_coords_from_doc(doc: fitz.Document, enable_ocr: bool = True) -> tuple[float | None, float | None, int | None, str]:
    """
    1) Text pass (fast, reliable for most)
    2) Lightweight OCR pass on first few pages (optional)
    """
    # 1) Text pass
    for i in range(min(12, len(doc))):
        txt = doc[i].get_text("text") or ""
        lat, lon = extract_coords_from_text(txt)
        if lat is not None:
            return lat, lon, i + 1, "text"

    # 2) OCR pass
    if enable_ocr:
        for i in range(min(6, len(doc))):
            lat, lon = ocr_coords_general(doc[i])
            if lat is not None:
                return lat, lon, i + 1, "ocr"

    return None, None, None, "not_found"

# =============================
# CLI + MAIN
# =============================

def parse_args() -> argparse.Namespace:
    default_root = r"I:\My Drive\2025 South Carolina TEB"
    default_tab1 = "TAB 1 - Application"

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    default_output_dir = os.path.join(project_root, "output")
    default_log_dir = os.path.join(project_root, "logs")

    p = argparse.ArgumentParser(
        description="Extract SC TEB application centroid coordinates (lat/lon) from project PDFs."
    )
    p.add_argument("--root", default=default_root, help="Root folder containing project subfolders.")
    p.add_argument("--tab1", default=default_tab1, help='TAB 1 folder name (default: "TAB 1 - Application").')
    p.add_argument("--output-dir", default=default_output_dir, help="Output directory for CSVs.")
    p.add_argument("--log-dir", default=default_log_dir, help="Directory for run logs.")
    p.add_argument("--no-ocr", action="store_true", help="Disable OCR fallback (text-only).")

    return p.parse_args()


def main():
    args = parse_args()

    root = args.root
    tab1_folder_name = args.tab1
    output_dir = os.path.abspath(args.output_dir)
    log_dir = os.path.abspath(args.log_dir)
    enable_ocr = not args.no_ocr

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    run_ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    output_csv = os.path.join(output_dir, f"SC_TEB_Coordinates_EXTRACTED_{run_ts}.csv")
    latest_csv = os.path.join(output_dir, "SC_TEB_Coordinates_EXTRACTED_latest.csv")
    log_path = os.path.join(log_dir, f"run_{run_ts}.log")

    # NEW: Miss-only CSV paths
    misses_csv = os.path.join(output_dir, f"SC_TEB_Coordinates_MISSES_{run_ts}.csv")
    misses_latest_csv = os.path.join(output_dir, "SC_TEB_Coordinates_MISSES_latest.csv")

    # Setup tee logging safely
    original_stdout = sys.stdout
    log_file = open(log_path, "w", encoding="utf-8")
    sys.stdout = Tee(original_stdout, log_file)

    try:
        print("Root:", root)
        print("TAB 1 folder name:", tab1_folder_name)
        print("Output dir:", output_dir)
        print("Log dir:", log_dir)
        print("OCR enabled:", enable_ocr)
        print("")

        rows = []

        if not os.path.isdir(root):
            raise FileNotFoundError(f'ROOT folder not found: "{root}"')

        for project in sorted(os.listdir(root)):
            project_path = os.path.join(root, project)
            if not os.path.isdir(project_path):
                continue

            tab1_path = os.path.join(project_path, tab1_folder_name)
            if not os.path.isdir(tab1_path):
                rows.append({
                    "project_folder": project,
                    "pdf_path": "",
                    "latitude": "",
                    "longitude": "",
                    "coords_page": "",
                    "mode": "",
                    "status": "Missing TAB 1 folder"
                })
                continue

            pdf_path = pick_best_application_pdf(tab1_path)
            if not pdf_path:
                rows.append({
                    "project_folder": project,
                    "pdf_path": "",
                    "latitude": "",
                    "longitude": "",
                    "coords_page": "",
                    "mode": "",
                    "status": "No PDF found"
                })
                continue

            try:
                doc = fitz.open(pdf_path)
                lat, lon, page_num, mode = extract_coords_from_doc(doc, enable_ocr=enable_ocr)
                doc.close()

                if lat is not None:
                    print(f"[OK] {project} -> {lat}, {lon} ({mode})")
                    rows.append({
                        "project_folder": project,
                        "pdf_path": pdf_path,
                        "latitude": lat,
                        "longitude": lon,
                        "coords_page": page_num,
                        "mode": mode,
                        "status": "OK"
                    })
                else:
                    print(f"[MISS] {project} (no coords) | {os.path.basename(pdf_path)}")
                    rows.append({
                        "project_folder": project,
                        "pdf_path": pdf_path,
                        "latitude": "",
                        "longitude": "",
                        "coords_page": "",
                        "mode": mode,
                        "status": "MISS (coords not found)"
                    })

            except Exception as e:
                print(f"[ERROR] {project} -> {e}")
                rows.append({
                    "project_folder": project,
                    "pdf_path": pdf_path,
                    "latitude": "",
                    "longitude": "",
                    "coords_page": "",
                    "mode": "",
                    "status": f"ERROR: {e}"
                })

        df = pd.DataFrame(rows)

        # Write main outputs
        df.to_csv(output_csv, index=False)
        df.to_csv(latest_csv, index=False)

        # NEW: Write misses-only outputs
        misses = df[df["status"].astype(str) != "OK"].copy()
        misses.to_csv(misses_csv, index=False)
        misses.to_csv(misses_latest_csv, index=False)

        print("\nCSV written to:", output_csv)
        print("\nStatus counts:")
        print(df["status"].value_counts())

        print("\nLatest CSV updated:", latest_csv)
        print("Log written to:", log_path)

        print("\nMisses CSV written to:", misses_csv)
        print("Misses latest updated:", misses_latest_csv)

    finally:
        # Restore stdout BEFORE closing log file
        sys.stdout = original_stdout
        log_file.close()


if __name__ == "__main__":
    main()