import os
import re
import csv
import time
import zipfile
import argparse
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


# ----------------------------
# Helpers
# ----------------------------

def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def safe_filename(name: str) -> str:
    name = name.strip().replace("\n", " ")
    name = re.sub(r"[<>:\"/\\|?*\x00-\x1F]", "_", name)
    name = re.sub(r"\s+", " ", name)
    return name


def ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


def default_url_for_year(year: int) -> str:
    # If the site changes naming conventions, pass --urls instead.
    return f"https://schousing.sc.gov/{year}-teb-applications"


def is_zip_url(url: str) -> bool:
    return ".zip" in url.lower()


def extract_zip(zip_path: str, extract_dir: str) -> tuple[bool, str]:
    """
    Extracts zip into extract_dir/<zip_basename_without_ext>/
    Returns (ok, extracted_folder_path or error message)
    """
    base = os.path.splitext(os.path.basename(zip_path))[0]
    out_dir = os.path.join(extract_dir, safe_filename(base))
    ensure_dir(out_dir)

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(out_dir)
        return True, out_dir
    except Exception as e:
        return False, f"unzip_error: {e}"


@dataclass
class DownloadResult:
    year: int
    page_url: str
    file_url: str
    filename: str
    zip_path: str
    extracted_path: str
    status: str
    bytes: int
    attempts: int
    error: str
    timestamp: str


# ----------------------------
# Scrape ZIP links
# ----------------------------

def scrape_zip_links(page_url: str, timeout: int = 30) -> list[str]:
    """
    Fetches page_url and returns absolute URLs of ZIP links found on the page.
    """
    r = requests.get(page_url, timeout=timeout)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue
        full = urljoin(page_url, href)
        if is_zip_url(full):
            links.append(full)

    # de-dupe while preserving order
    seen = set()
    out = []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# ----------------------------
# Download with retry
# ----------------------------

def download_with_retry(
    url: str,
    dest_path: str,
    timeout: int,
    max_retries: int,
    backoff_base: float,
    chunk_size: int = 1024 * 256,  # 256KB
) -> tuple[bool, int, int, str]:
    """
    Returns (ok, bytes_written, attempts_used, error_message)
    Retries on network errors and HTTP 5xx / 429.
    """
    ensure_dir(os.path.dirname(dest_path))
    last_err = ""
    attempts = 0

    for i in range(1, max_retries + 1):
        attempts = i
        try:
            # fresh session per attempt (thread-safe)
            with requests.Session() as s:
                with s.get(url, stream=True, timeout=timeout) as resp:
                    status = resp.status_code
                    if status >= 400:
                        # retry for common transient codes
                        if status in (429, 500, 502, 503, 504):
                            raise requests.HTTPError(f"HTTP {status} transient", response=resp)
                        resp.raise_for_status()

                    tmp_path = dest_path + ".part"
                    bytes_written = 0
                    with open(tmp_path, "wb") as f:
                        for chunk in resp.iter_content(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                                bytes_written += len(chunk)

                    # atomic-ish replace
                    os.replace(tmp_path, dest_path)
                    return True, bytes_written, attempts, ""
        except Exception as e:
            last_err = str(e)
            # backoff
            if i < max_retries:
                sleep_s = backoff_base * (2 ** (i - 1))
                time.sleep(sleep_s)

    return False, 0, attempts, last_err


# ----------------------------
# Worker
# ----------------------------

def process_one_zip(
    year: int,
    page_url: str,
    file_url: str,
    year_dir: str,
    unzip: bool,
    timeout: int,
    max_retries: int,
    backoff_base: float,
) -> DownloadResult:
    ts = now_iso()

    parsed = urlparse(file_url)
    filename = os.path.basename(parsed.path) or "download.zip"
    filename = safe_filename(filename)
    zip_dir = ensure_dir(os.path.join(year_dir, "zips"))
    extract_dir = ensure_dir(os.path.join(year_dir, "extracted"))

    zip_path = os.path.join(zip_dir, filename)

    # If already downloaded, skip download
    if os.path.exists(zip_path) and os.path.getsize(zip_path) > 0:
        bytes_on_disk = os.path.getsize(zip_path)
        if unzip:
            ok_unzip, unzip_out = extract_zip(zip_path, extract_dir)
            if ok_unzip:
                return DownloadResult(
                    year, page_url, file_url, filename, zip_path, unzip_out,
                    "SKIP_DOWNLOADED_UNZIPPED", bytes_on_disk, 0, "", ts
                )
            return DownloadResult(
                year, page_url, file_url, filename, zip_path, "",
                "SKIP_DOWNLOADED_UNZIP_FAIL", bytes_on_disk, 0, unzip_out, ts
            )

        return DownloadResult(
            year, page_url, file_url, filename, zip_path, "",
            "SKIP_DOWNLOADED", bytes_on_disk, 0, "", ts
        )

    ok, bytes_written, attempts, err = download_with_retry(
        file_url, zip_path, timeout=timeout, max_retries=max_retries, backoff_base=backoff_base
    )

    if not ok:
        return DownloadResult(
            year, page_url, file_url, filename, zip_path, "",
            "DOWNLOAD_FAIL", 0, attempts, err, ts
        )

    if unzip:
        ok_unzip, unzip_out = extract_zip(zip_path, extract_dir)
        if ok_unzip:
            return DownloadResult(
                year, page_url, file_url, filename, zip_path, unzip_out,
                "DOWNLOADED_UNZIPPED", bytes_written, attempts, "", ts
            )
        return DownloadResult(
            year, page_url, file_url, filename, zip_path, "",
            "DOWNLOADED_UNZIP_FAIL", bytes_written, attempts, unzip_out, ts
        )

    return DownloadResult(
        year, page_url, file_url, filename, zip_path, "",
        "DOWNLOADED", bytes_written, attempts, "", ts
    )


# ----------------------------
# CSV logging
# ----------------------------

LOG_FIELDS = [
    "timestamp",
    "year",
    "page_url",
    "file_url",
    "filename",
    "status",
    "bytes",
    "attempts",
    "zip_path",
    "extracted_path",
    "error",
]

def write_csv(path: str, rows: list[DownloadResult]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=LOG_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({
                "timestamp": r.timestamp,
                "year": r.year,
                "page_url": r.page_url,
                "file_url": r.file_url,
                "filename": r.filename,
                "status": r.status,
                "bytes": r.bytes,
                "attempts": r.attempts,
                "zip_path": r.zip_path,
                "extracted_path": r.extracted_path,
                "error": r.error,
            })


# ----------------------------
# CLI
# ----------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Bulk-download SC TEB 4% application ZIPs, optionally unzip, with retries, progress bar, parallelism, and CSV logs."
    )
    p.add_argument("--years", nargs="*", type=int, default=[2025],
                   help="Years to download (e.g., --years 2025 2026). Ignored if --urls is provided for the same count.")
    p.add_argument("--urls", nargs="*", default=[],
                   help='Explicit page URLs (e.g., --urls "https://schousing.sc.gov/2025-teb-applications"). Overrides year->URL mapping for provided entries.')
    p.add_argument("--base-dir", default=r"C:\Users\tanne\Downloads\SC_TEB",
                   help="Base download directory. Year folders will be created under this directory.")
    p.add_argument("--workers", type=int, default=6, help="Parallel download workers.")
    p.add_argument("--timeout", type=int, default=60, help="HTTP timeout (seconds).")
    p.add_argument("--retries", type=int, default=4, help="Max retries per file.")
    p.add_argument("--backoff", type=float, default=1.0, help="Backoff base seconds (exponential).")
    p.add_argument("--no-unzip", action="store_true", help="Disable unzip after download.")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    unzip = not args.no_unzip
    base_dir = os.path.abspath(args.base_dir)
    ensure_dir(base_dir)

    # Build (year, page_url) list
    pairs: list[tuple[int, str]] = []
    if args.urls:
        # If URLs provided without years, infer year=0
        if len(args.years) == 0:
            years = [0] * len(args.urls)
        else:
            # If counts mismatch, still proceed: zip min; extras get default mapping
            years = list(args.years)
            if len(years) < len(args.urls):
                years += [0] * (len(args.urls) - len(years))
        pairs = list(zip(years[:len(args.urls)], args.urls))
        # Add any extra years not covered by URLs
        if len(args.years) > len(args.urls):
            for y in args.years[len(args.urls):]:
                pairs.append((y, default_url_for_year(y)))
    else:
        pairs = [(y, default_url_for_year(y)) for y in args.years]

    all_results: list[DownloadResult] = []

    for year, page_url in pairs:
        year_label = str(year) if year else "unknown_year"
        year_dir = ensure_dir(os.path.join(base_dir, year_label))
        log_dir = ensure_dir(os.path.join(year_dir, "logs"))

        print(f"\n=== {year_label} ===")
        print(f"Page: {page_url}")
        print(f"Base dir: {year_dir}")
        print(f"Unzip: {unzip} | Workers: {args.workers} | Retries: {args.retries}\n")

        try:
            zip_links = scrape_zip_links(page_url, timeout=args.timeout)
        except Exception as e:
            # log a single row for the page failure
            fail = DownloadResult(
                year=year if year else -1,
                page_url=page_url,
                file_url="",
                filename="",
                zip_path="",
                extracted_path="",
                status="PAGE_FETCH_FAIL",
                bytes=0,
                attempts=0,
                error=str(e),
                timestamp=now_iso(),
            )
            year_results = [fail]
            all_results.extend(year_results)
            year_log = os.path.join(log_dir, f"download_log_{year_label}_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.csv")
            write_csv(year_log, year_results)
            print(f"[ERROR] Failed to fetch page. Logged to: {year_log}")
            continue

        if not zip_links:
            print("[WARN] No ZIP links found on page.")
            continue

        year_results: list[DownloadResult] = []

        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = [
                ex.submit(
                    process_one_zip,
                    year if year else -1,
                    page_url,
                    link,
                    year_dir,
                    unzip,
                    args.timeout,
                    args.retries,
                    args.backoff,
                )
                for link in zip_links
            ]

            for fut in tqdm(as_completed(futures), total=len(futures), desc=f"{year_label} downloads"):
                res = fut.result()
                year_results.append(res)

        # Write per-year log
        year_log = os.path.join(log_dir, f"download_log_{year_label}_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.csv")
        write_csv(year_log, sorted(year_results, key=lambda r: r.filename))
        all_results.extend(year_results)

        # Summary
        counts = {}
        for r in year_results:
            counts[r.status] = counts.get(r.status, 0) + 1

        print("\nStatus counts:")
        for k in sorted(counts.keys()):
            print(f"  {k}: {counts[k]}")
        print(f"\nCSV log written: {year_log}")

    # Combined log
    combined_log = os.path.join(base_dir, f"download_log_ALL_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.csv")
    write_csv(combined_log, sorted(all_results, key=lambda r: (r.year, r.filename)))
    print(f"\nCombined CSV log written: {combined_log}")


if __name__ == "__main__":
    main()