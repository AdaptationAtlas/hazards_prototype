#!/usr/bin/env python3
"""
_knbs_admin.py — shared helpers for the two KNBS population ingests (KE-39 / issue #28).

Imported by:
  ingest_population_knbs_census.py       (2019 Census of Population and Housing counts)
  ingest_population_knbs_projections.py  (Analytical Report on Population Projections, Vol XVI)

Holds the three things both scripts need and neither should re-invent:

1. COUNTY_PCODE — the KNBS county name -> IEBC COD-AB adm1_pcode crosswalk. KNBS and COD-AB spell
   several counties differently (ELGEYO/MARAKWET vs Elgeyo-Marakwet, NAIROBI CITY vs Nairobi,
   TAITA/TAVETA vs Taita Taveta) and the Vol XVI PDF additionally line-wraps "Elgeiyo Marakwet" so
   only "Marakwet" survives text extraction. Normalising to alphanumerics plus a short alias list
   resolves all 47. Counties are the ONLY admin level where KNBS and COD-AB agree unit-for-unit:
   sub-county does NOT join (COD-AB has 290 IEBC constituencies, the census has 349 KNBS
   sub-counties incl. forests/parks; only 183 names match). See the module note in
   ingest_population_knbs_census.py.

2. A dependency-free .xlsx reader. The KNBS census workbook is a plain SpreadsheetML zip; reading
   it with zipfile+ElementTree keeps these two scripts free of pandas/openpyxl, which the rest of
   the exposure ingests do not need either.

3. write_table() — parquet out via pyarrow, CSV fallback with a loud warning if pyarrow is absent.
   The notebook reads parquet over DuckDB-WASM, so parquet is the publishable form.
"""
import datetime as dt
import re
import zipfile
from xml.etree import ElementTree as ET

MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"


def explain_tls_failure(exc, url):
    """KNBS serves an incomplete certificate chain (leaf only, no issuing intermediate), so
    strict verification fails with code 21 even though the certificate is genuine. Do NOT
    disable verification, and do NOT vendor the intermediate into this repo - it expires.
    Complete the chain from the leaf's own AIA extension and keep verification on.
    Recorded 2026-09-17 by cglabs, who hit this on the first real run."""
    return (
        "TLS verification failed for {}: {}\n".format(url, exc)
        + "  The KNBS server sends its leaf certificate without the issuing intermediate, so the\n"
          "  chain cannot be built locally. The certificate is genuine; this is a server\n"
          "  misconfiguration, not an interception.\n"
          "  Fix it without weakening TLS - fetch the intermediate named in the leaf's AIA\n"
          "  extension, append it to a trust bundle, and point SSL_CERT_FILE at that bundle:\n"
          "    openssl s_client -connect www.knbs.or.ke:443 -servername www.knbs.or.ke </dev/null \\\n"
          "      2>/dev/null | openssl x509 -noout -text | grep -A1 'CA Issuers'\n"
          "    curl -s <that URL> | openssl x509 -inform DER -out /tmp/knbs_intermediate.pem\n"
          "    cat \"$(python3 -c 'import certifi;print(certifi.where())')\" /tmp/knbs_intermediate.pem \\\n"
          "      > /tmp/knbs_bundle.pem\n"
          "    SSL_CERT_FILE=/tmp/knbs_bundle.pem python3 <this script>\n"
          "  Never substitute an unverified SSL context."
    )


def log(msg):
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# County crosswalk: KNBS census order (= KE001..KE047) -> COD-AB adm1
# ---------------------------------------------------------------------------
# (pcode, COD-AB adm1_name, KNBS census spelling). Census order is significant: both sources list
# counties in this order, so position is used as an independent check on the name match.
COUNTIES = [
    ("KE001", "Mombasa", "MOMBASA"),
    ("KE002", "Kwale", "KWALE"),
    ("KE003", "Kilifi", "KILIFI"),
    ("KE004", "Tana River", "TANA RIVER"),
    ("KE005", "Lamu", "LAMU"),
    ("KE006", "Taita Taveta", "TAITA/TAVETA"),
    ("KE007", "Garissa", "GARISSA"),
    ("KE008", "Wajir", "WAJIR"),
    ("KE009", "Mandera", "MANDERA"),
    ("KE010", "Marsabit", "MARSABIT"),
    ("KE011", "Isiolo", "ISIOLO"),
    ("KE012", "Meru", "MERU"),
    ("KE013", "Tharaka-Nithi", "THARAKA-NITHI"),
    ("KE014", "Embu", "EMBU"),
    ("KE015", "Kitui", "KITUI"),
    ("KE016", "Machakos", "MACHAKOS"),
    ("KE017", "Makueni", "MAKUENI"),
    ("KE018", "Nyandarua", "NYANDARUA"),
    ("KE019", "Nyeri", "NYERI"),
    ("KE020", "Kirinyaga", "KIRINYAGA"),
    ("KE021", "Murang'a", "MURANG'A"),
    ("KE022", "Kiambu", "KIAMBU"),
    ("KE023", "Turkana", "TURKANA"),
    ("KE024", "West Pokot", "WEST POKOT"),
    ("KE025", "Samburu", "SAMBURU"),
    ("KE026", "Trans Nzoia", "TRANS NZOIA"),
    ("KE027", "Uasin Gishu", "UASIN GISHU"),
    ("KE028", "Elgeyo-Marakwet", "ELGEYO/MARAKWET"),
    ("KE029", "Nandi", "NANDI"),
    ("KE030", "Baringo", "BARINGO"),
    ("KE031", "Laikipia", "LAIKIPIA"),
    ("KE032", "Nakuru", "NAKURU"),
    ("KE033", "Narok", "NAROK"),
    ("KE034", "Kajiado", "KAJIADO"),
    ("KE035", "Kericho", "KERICHO"),
    ("KE036", "Bomet", "BOMET"),
    ("KE037", "Kakamega", "KAKAMEGA"),
    ("KE038", "Vihiga", "VIHIGA"),
    ("KE039", "Bungoma", "BUNGOMA"),
    ("KE040", "Busia", "BUSIA"),
    ("KE041", "Siaya", "SIAYA"),
    ("KE042", "Kisumu", "KISUMU"),
    ("KE043", "Homa Bay", "HOMA BAY"),
    ("KE044", "Migori", "MIGORI"),
    ("KE045", "Kisii", "KISII"),
    ("KE046", "Nyamira", "NYAMIRA"),
    ("KE047", "Nairobi", "NAIROBI CITY"),
]

# Spellings that do not normalise onto a COD-AB name by themselves.
# "MARAKWET" / "ELGEIYOMARAKWET": the Vol XVI PDF wraps "Elgeiyo Marakwet" across two lines, so the
# table header line carries only the second word.
COUNTY_ALIASES = {
    "MARAKWET": "KE028",
    "ELGEIYOMARAKWET": "KE028",
    "ELGEIYO": "KE028",
    "NAIROBICITY": "KE047",
    "NAIROBI": "KE047",
}


def norm_name(s):
    """Uppercase, alphanumerics only — collapses '/', '-', ' ' and apostrophe spelling differences."""
    return re.sub(r"[^A-Z0-9]", "", (s or "").upper())


COUNTY_PCODE = {norm_name(knbs): pcode for pcode, _codab, knbs in COUNTIES}
COUNTY_PCODE.update({norm_name(codab): pcode for pcode, codab, _knbs in COUNTIES})
COUNTY_PCODE.update(COUNTY_ALIASES)
PCODE_NAME = {pcode: codab for pcode, codab, _knbs in COUNTIES}


def resolve_county(name):
    """KNBS/PDF county label -> (adm1_pcode, COD-AB adm1_name). Raises if unresolved."""
    key = norm_name(name)
    pcode = COUNTY_PCODE.get(key)
    if pcode is None:
        raise KeyError(f"county '{name}' (normalised '{key}') is not in the KNBS -> COD-AB crosswalk")
    return pcode, PCODE_NAME[pcode]


def check_county_order(pcodes, label):
    """Hard gate: the 47 resolved counties must be complete, unique and in census (pcode) order."""
    expect = [c[0] for c in COUNTIES]
    if pcodes != expect:
        missing = [p for p in expect if p not in pcodes]
        extra = [p for p in pcodes if p not in expect]
        raise RuntimeError(
            f"{label}: county sequence does not match COD-AB KE001..KE047 "
            f"(got {len(pcodes)}, missing {missing}, unexpected {extra})"
        )


# ---------------------------------------------------------------------------
# minimal .xlsx reader (no pandas / openpyxl)
# ---------------------------------------------------------------------------
def _sheet_paths(zf):
    wb = ET.fromstring(zf.read("xl/workbook.xml"))
    rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
    rmap = {r.get("Id"): r.get("Target") for r in rels}
    out = {}
    for s in wb.find(f"{{{MAIN_NS}}}sheets"):
        target = rmap[s.get(f"{{{REL_NS}}}id")].lstrip("/")
        out[s.get("name")] = target if target.startswith("xl/") else "xl/" + target
    return out


def _shared_strings(zf):
    if "xl/sharedStrings.xml" not in zf.namelist():
        return []
    root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    return ["".join(t.text or "" for t in si.iter(f"{{{MAIN_NS}}}t")) for si in root]


def _col_index(ref):
    n = 0
    for ch in re.match(r"[A-Z]+", ref).group():
        n = n * 26 + ord(ch) - 64
    return n - 1


def xlsx_sheets(path):
    with zipfile.ZipFile(path) as zf:
        return list(_sheet_paths(zf))


def xlsx_table(path, sheet):
    """Return (header_row, data_rows) as lists of str/None, padded to a rectangle."""
    with zipfile.ZipFile(path) as zf:
        paths = _sheet_paths(zf)
        if sheet not in paths:
            raise KeyError(f"sheet '{sheet}' not in {path} (have: {list(paths)})")
        strings = _shared_strings(zf)
        root = ET.fromstring(zf.read(paths[sheet]))
    raw = []
    for row in root.iter(f"{{{MAIN_NS}}}row"):
        cells = {}
        for c in row:
            v = c.find(f"{{{MAIN_NS}}}v")
            if v is None:
                inline = c.find(f"{{{MAIN_NS}}}is")
                val = (
                    "".join(x.text or "" for x in inline.iter(f"{{{MAIN_NS}}}t"))
                    if inline is not None
                    else None
                )
            else:
                val = strings[int(v.text)] if c.get("t") == "s" else v.text
            cells[_col_index(c.get("r"))] = val
        raw.append(cells)
    if not raw:
        return [], []
    ncol = max((max(c) + 1) for c in raw if c)
    rect = [[c.get(i) for i in range(ncol)] for c in raw]
    return rect[0], rect[1:]


# ---------------------------------------------------------------------------
# output
# ---------------------------------------------------------------------------
def write_table(rows, columns, path, fmt="parquet"):
    """Write list-of-dicts `rows` with column order `columns`. parquet (pyarrow) or csv."""
    if fmt == "parquet":
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            log("WARNING: pyarrow not importable — falling back to CSV. The notebook reads parquet; "
                "install pyarrow (or convert with R/arrow) before publishing.")
            fmt = "csv"
        else:
            table = pa.table({c: [r.get(c) for r in rows] for c in columns})
            pq.write_table(table, path, compression="snappy")
            log(f"  wrote {path} ({len(rows)} rows, {len(columns)} cols, parquet/snappy)")
            return path
    import csv

    path = re.sub(r"\.parquet$", ".csv", path)
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=columns)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c) for c in columns})
    log(f"  wrote {path} ({len(rows)} rows, {len(columns)} cols, csv)")
    return path
