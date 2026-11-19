#!/usr/bin/env python3
"""Build metadata/cdh/submission-log.csv: every CDH record the Atlas / CGlabs work has authored,
and where each one stands in CGIAR-Climate-Data-Hub/cdh-catalog.

One row per record id. Live columns are rebuilt on every run from the record files and from
GitHub (read-only `gh` calls; nothing is posted). Hand-kept columns -- `group`, `catalog_id`,
`hold`, `note` -- are carried over from the existing CSV, matched on `record_id`.

    python3 scripts/cdh_submission_log.py              # from the repo root; needs `gh` signed in
    python3 scripts/cdh_submission_log.py --offline    # local columns only, keep the last live values

Sources of records (edit SOURCES to add one):
  - this repo, metadata/cdh/*.yaml (strict) and metadata/cdh/draft/*.yaml;
  - AdaptationAtlas/atlas_notebooks, KE-ENSO explorer records (read from GitHub, not a local path).

A record is matched to catalog PRs by the `records/<id>/` paths each PR touches. When the
catalog renamed a record (e.g. eastafrica-flood-jrc -> jrc-glofas), put the catalog id in the
`catalog_id` column and the match follows it.
"""

import argparse
import base64
import csv
import datetime as dt
import json
import os
import pathlib
import subprocess
import sys

import yaml

CATALOG = "CGIAR-Climate-Data-Hub/cdh-catalog"
ROOT = pathlib.Path(__file__).resolve().parent.parent
OUT = ROOT / "metadata" / "cdh" / "submission-log.csv"

# (label, kind, location). kind "local" = glob under this repo; "github" = repo@ref:dir
SOURCES = [
    ("hazards_prototype", "local", "metadata/cdh/*.yaml"),
    ("hazards_prototype", "local", "metadata/cdh/draft/*.yaml"),
    ("atlas_notebooks", "github",
     "AdaptationAtlas/atlas_notebooks@dev/KE-enso-explorer:data/KE-enso-explorer/_sources/cdh_local"),
]

HAND = ["group", "catalog_id", "hold", "note"]
LIVE = ["source", "path", "schema_version", "record_version", "tier", "access",
        "pr", "pr_state", "pr_opened", "pr_closed", "other_prs",
        "in_catalog", "catalog_schema_version", "catalog_version", "checked"]
COLUMNS = ["record_id"] + HAND[:1] + LIVE + HAND[1:]


def gh(*args):
    r = subprocess.run(["gh", *args], capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"gh {' '.join(args)} failed: {r.stderr.strip()}")
    return r.stdout


def gh_file(repo, ref, path):
    out = gh("api", f"repos/{repo}/contents/{path}?ref={ref}")
    return base64.b64decode(json.loads(out)["content"]).decode()


def summarise(text, source, path):
    d = yaml.safe_load(text) or {}
    return {
        "record_id": d.get("id") or pathlib.Path(path).stem,
        "source": source,
        "path": path,
        "schema_version": d.get("cdh_schema_version", ""),
        "record_version": str(d.get("version", "")),
        "tier": "draft" if "/draft/" in f"/{path}" else "strict",
        "access": d.get("access", "public"),
    }


def local_records(offline):
    recs = []
    for label, kind, loc in SOURCES:
        if kind == "local":
            for f in sorted(ROOT.glob(loc)):
                recs.append(summarise(f.read_text(), label, str(f.relative_to(ROOT))))
        else:
            repo_ref, d = loc.split(":", 1)
            repo, ref = repo_ref.split("@", 1)
            # A local checkout wins when named, so unpushed records are logged too:
            #   CDH_LOG_ATLAS_NOTEBOOKS=/path/to/atlas_notebooks python3 scripts/cdh_submission_log.py
            checkout = os.environ.get(f"CDH_LOG_{label.upper()}")
            if checkout:
                for f in sorted(pathlib.Path(checkout, d).glob("*.y*ml")):
                    recs.append(summarise(f.read_text(), label,
                                          f"{repo}@{ref}:{d}/{f.name} (local checkout)"))
                continue
            if offline:
                continue
            listing = json.loads(gh("api", f"repos/{repo}/contents/{d}?ref={ref}"))
            for item in listing:
                if item["name"].endswith((".yaml", ".yml")):
                    recs.append(summarise(gh_file(repo, ref, item["path"]), label,
                                          f"{repo}@{ref}:{item['path']}"))
    return recs


def catalog_state():
    """PRs (any author) touching records/<id>/, and the records on main with their versions."""
    prs = json.loads(gh("pr", "list", "-R", CATALOG, "--state", "all", "--limit", "500",
                        "--json", "number,state,createdAt,mergedAt,closedAt,files,author"))
    by_id = {}
    for p in prs:
        ids = {f["path"].split("/")[1] for f in p["files"] if f["path"].startswith("records/")}
        for i in ids:
            by_id.setdefault(i, []).append(p)
    tree = json.loads(gh("api", f"repos/{CATALOG}/git/trees/main?recursive=1"))["tree"]
    on_main = {}
    for t in tree:
        parts = t["path"].split("/")
        if len(parts) == 3 and parts[0] == "records" and parts[2] == f"{parts[1]}.yaml":
            on_main[parts[1]] = t["path"]
    return by_id, on_main


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--offline", action="store_true", help="no GitHub calls")
    a = ap.parse_args()

    old = {}
    if OUT.exists():
        with OUT.open(newline="") as fh:
            old = {r["record_id"]: r for r in csv.DictReader(fh)}

    recs = local_records(a.offline)
    seen = set()
    rows = []
    if not a.offline:
        by_id, on_main = catalog_state()
    today = dt.date.today().isoformat()

    for r in recs:
        rid = r["record_id"]
        if rid in seen:
            continue
        seen.add(rid)
        prev = old.get(rid, {})
        row = {c: prev.get(c, "") for c in COLUMNS}
        row.update(r)
        if not a.offline:
            cid = prev.get("catalog_id") or rid
            prs = sorted(by_id.get(cid, []), key=lambda p: p["createdAt"])
            # the PR that added the record: first one by us, else the first one at all
            ours = [p for p in prs if p["author"]["login"] != "bjyberg"] or prs
            first = ours[0] if ours else None
            row["pr"] = f"#{first['number']}" if first else ""
            row["pr_state"] = first["state"].lower() if first else ""
            row["pr_opened"] = first["createdAt"][:10] if first else ""
            row["pr_closed"] = ((first.get("mergedAt") or first.get("closedAt") or "")[:10]
                                if first else "")
            row["other_prs"] = " ".join(f"#{p['number']}" for p in prs if p is not first)
            row["in_catalog"] = "yes" if cid in on_main else "no"
            if cid in on_main:
                d = yaml.safe_load(gh_file(CATALOG, "main", on_main[cid])) or {}
                row["catalog_schema_version"] = d.get("cdh_schema_version", "")
                row["catalog_version"] = str(d.get("version", ""))
            else:
                row["catalog_schema_version"] = row["catalog_version"] = ""
            row["checked"] = today
        rows.append(row)

    # keep hand-added rows whose record file has gone (retired, renamed) so history is not lost
    for rid, prev in old.items():
        if rid not in seen:
            prev = {c: prev.get(c, "") for c in COLUMNS}
            prev["note"] = prev["note"] or "record file not found on last run"
            rows.append(prev)

    rows.sort(key=lambda r: (r["group"] or "~", r["record_id"]))
    with OUT.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {OUT.relative_to(ROOT)}: {len(rows)} records")


if __name__ == "__main__":
    main()
