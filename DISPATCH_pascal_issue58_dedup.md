# DISPATCH — pascal — #58 storage deduplication

**Append-only; newest block on top. Prepend `### RESPONSE — pascal [<code>]` to answer.**

This file belongs to the **deduplication** PASCAL session. The migration session answers in
`DISPATCH_pascal_issue29_followups.md` and does not answer here. Prompt codes in this file are
`M-YYYYMMDD-i58dup-<n>`, so they cannot collide with the migration thread's `i58` codes.

Standing rules for every block in this file:
- **Never open, hash or copy another user's files**, except under the shared climate root `$CL` when a block
  says so. Groups owned by other people go to IT or to the owner; Pete identifies owners from the private
  mapping on the node.
- **Usernames and real paths stay on the node**, in git-ignored `logs/p15_*` and `logs/dup_*`. The RESPONSE uses
  the census tags (`owner-NN`, `self`) and paths as `$B/…`, `$W/…`, `$CL/…`, `$CD/…`. No hostnames or IPs.
- Read-only outside `logs/`: no `rm`, `mv`, `cp` or `chmod` of anything there. The only deletes allowed are of
  temporary files the block itself created under `logs/`.
- Long reads at idle I/O priority (`ionice -c3 nice -n 19`), one hashing stream at a time.

---

### MACBOOK 2026-11-19 [M-20261119-i58dup-1] — DUP1: confirm by content what we may hash; measure what compression would save; private contact list for the rest. **Small profile. Mostly one `md5sum` stream over about 40 GB, plus three test rewrites.**

**Why.** P15 (`[M-20261119-i58-4]`, RESPONSE in `DISPATCH_pascal_issue29_followups.md`) listed 711 GB of
probable duplicates by name and size, about 245 GB after known false matches, **1.0 % of the 24.57 TB used**. So
removing duplicates will not free much space. This block does three things:
1. **Confirms by md5** the groups we are allowed to hash: those that sit wholly in the shared climate tree `$CL`, and any of our own (`self`)
   groups apart from `hazard_risk/annual` vs `…/jagermeyr`. That pair is already settled: P15 hashed three pairs and all three
   differ, so it is recorded as a false match and not hashed again.
2. **Measures our own uncompressed rasters.** P15 found the `hazard_risk` rasters are written uncompressed (sizes
   depend only on band count). That is about 2 TB of our own data. If lossless compression shrinks it a lot, that saves more than all the duplicates together.
3. **Writes a private, per-owner contact list** of the other people's groups, so Pete can contact owners or IT. Only
   counts and GB per tag go in the RESPONSE.

It also answers one question P15 raised: `$B/atlas/hazards_prototype` holds 4.64 TB but `self` owns 2.24 TB. Who owns
the rest of our area (by tag)?

#### K0 — pull, roots, the P15 outputs
```bash
cd <repo> && export ATLAS_PROJECT_DIR=$PWD
export B=<as in P15>  W=<as in P15>  CL=<as in P15>  CD=<as in P15>
export S=20261119_192126
git pull --ff-only && git log --oneline -1 && git rev-parse --abbrev-ref HEAD      # develop, at or after the DUP1 commit
git check-ignore -q logs/dup_test && echo "logs/dup_* ignored"                    # must print; else stop
export P=logs/p15_private_$S L=<absolute path of the P15 listing, p15_files_$S.tsv>
ls $P/owner_usage.tsv $P/dup_files.tsv $P/dup_trees.tsv && wc -l < $L             # 1,096,294 lines expected
```

#### K1 — who owns our area (tags only in the RESPONSE)
```bash
awk -F'\t' -v w="$W/" 'NR==FNR { if (FNR>1) t[$1]=$2; next }
     index($3, w)==1 { b[t[$2]]+=$1; n[t[$2]]++ }
     END { for (k in b) printf "%s\t%d\t%.2f\n", k, n[k], b[k]/1e9 }' $P/owner_usage.tsv $L | sort -k3,3nr
awk -F'\t' -v w="$W/" 'NR==FNR { if (FNR>1) t[$1]=$2; next }
     index($3, w)==1 { split(substr($3, length(w)+1), a, "/"); k=t[$2]"\t"a[1]"/"a[2]; b[k]+=$1 }
     END { for (k in b) printf "%s\t%.2f\n", k, b[k]/1e9 }' $P/owner_usage.tsv $L | sort -k3,3nr | head -20
```
Report both tables as tags and `$W/…` paths (GB).

#### K2 — md5 of the `$CL` groups and the remaining `self` groups (background, idle I/O)
```bash
python3 -I - "$P/dup_files.tsv" "$CL" "$W" > logs/dup_k2_paths_$S.tsv <<'PY'
import sys, csv, collections
f, cl, w = sys.argv[1], sys.argv[2].rstrip("/") + "/", sys.argv[3].rstrip("/") + "/"
g = collections.defaultdict(list)
for r in csv.DictReader(open(f), delimiter="\t"):
    g[(r["name"], r["size"])].append(r["dir"])
for i, ((name, size), ds) in enumerate(sorted(g.items(), key=lambda kv: -int(kv[0][1]) * (len(kv[1]) - 1))):
    if all(d.startswith(cl) for d in ds): kind = "CL"
    elif all(d.startswith(w) for d in ds) and not any("/hazard_risk/jagermeyr" in d for d in ds): kind = "self"
    else: continue
    for d in ds: print(f"{i}\t{kind}\t{size}\t{d}/{name}")
PY
awk -F'\t' '{print $2}' logs/dup_k2_paths_$S.tsv | sort | uniq -c                     # files per kind
awk -F'\t' '{s[$2]+=$3} END {for (k in s) printf "%s %.2f GB to read\n", k, s[k]/1e9}' logs/dup_k2_paths_$S.tsv
```
**Stop and describe** if the `self` kind would read more than 100 GB, or `CL` more than 100 GB (P15 suggests about
36 GB of `$CL` groups, read twice ≈ 72 GB). Otherwise:
```bash
cat > logs/dup_k2_$S.sh <<'SH'
cut -f4 logs/dup_k2_paths_$S.tsv | tr '\n' '\0' | ionice -c3 nice -n 19 xargs -0 -r md5sum > logs/dup_k2_md5_$S.txt
echo "md5 exit=$?"
SH
nohup /usr/bin/time -v bash logs/dup_k2_$S.sh > logs/dup_k2_$S.log 2>&1 &
echo $! > logs/dup_k2_$S.pid
```
Afterwards, per group: `identical` (one md5 for every copy) or `DIFFERENT`. Report a table, largest first: group, kind,
copies, size each, verdict, and the copies as `$CL/…` / `$W/…` paths. Total confirmed-recoverable bytes per kind.
`Permission denied` is a finding, not a stop.

#### K3 — how much of our own data is written uncompressed
Our GeoTIFFs of 100 MB or more under `$W` only, header reads only:
```bash
awk -F'\t' -v w="$W/" -v me="$(id -un)" 'index($3, w)==1 && $2==me && $1>=1e8 && $4 ~ /\.tif$/ {print $1"\t"$3"/"$4}' $L > logs/dup_k3_tifs_$S.tsv
wc -l < logs/dup_k3_tifs_$S.tsv
Rscript -e 'S <- Sys.getenv("S"); x <- read.delim(paste0("logs/dup_k3_tifs_", S, ".tsv"), header = FALSE, col.names = c("bytes","path"), quote = "")
  cmp <- vapply(x$path, function(p) { d <- tryCatch(terra::describe(p), error = function(e) "ERR")
    m <- grep("COMPRESSION=", d, value = TRUE); if (length(m)) sub(".*COMPRESSION=", "", m[1]) else if (identical(d, "ERR")) "ERR" else "NONE" }, "")
  x$compression <- cmp; write.table(x, paste0("logs/dup_k3_compression_", S, ".tsv"), sep = "\t", row.names = FALSE, quote = FALSE)
  print(aggregate(bytes ~ compression, x, function(b) c(files = length(b), TB = sum(b) / 1e12)))'
```
Report the table (compression, files, TB) and the top-level `$W/…` folders holding the `NONE` bytes (GB each).

#### K4 — what lossless compression would save (three of our files)
Take the largest, the smallest and the 100th-by-name `.tif` in `$W/hazards_prototype/Data/hazard_risk/annual`, rewrite
each **into `logs/`** with ZSTD (fall back to DEFLATE if the GDAL build lacks ZSTD), check the values are identical, record the sizes, then delete the three test files:
```bash
Rscript -e 'library(terra)
  d <- file.path(Sys.getenv("W"), "hazards_prototype/Data/hazard_risk/annual")
  f <- list.files(d, "\\.tif$", full.names = TRUE, recursive = TRUE); s <- file.size(f)
  pick <- unique(c(f[which.max(s)], f[which.min(s)], sort(f)[min(100, length(f))]))
  for (p in pick) {
    r <- rast(p); dt <- datatype(r)[1]; pred <- if (grepl("^FLT", dt)) "PREDICTOR=3" else "PREDICTOR=2"
    o <- file.path("logs", paste0("dup_k4_", basename(p)))
    ok <- tryCatch({ writeRaster(r, o, datatype = dt, overwrite = TRUE,
                       gdal = c("COMPRESS=ZSTD", "ZSTD_LEVEL=9", pred, "TILED=YES")); "ZSTD" },
                   error = function(e) { writeRaster(r, o, datatype = dt, overwrite = TRUE,
                       gdal = c("COMPRESS=DEFLATE", "ZLEVEL=6", pred, "TILED=YES")); "DEFLATE" })
    m <- grep("COMPRESSION=", describe(o), value = TRUE); ok <- if (length(m)) sub(".*COMPRESSION=", "", m[1]) else "NONE"
    q <- rast(o); diff <- max(global(abs(r - q), "max", na.rm = TRUE)[, 1], na.rm = TRUE)
    na_same <- all(global(is.na(r), "sum")[, 1] == global(is.na(q), "sum")[, 1])
    cat(sprintf("%s\t%s\t%d bands\t%s\t%.0f MB -> %.0f MB (%.1f %%)\tmax|diff|=%g\tNA-same=%s\n",
        basename(p), dt, nlyr(r), ok, file.size(p)/1e6, file.size(o)/1e6, 100*file.size(o)/file.size(p), diff, na_same))
    unlink(o)
  }'
ls logs/dup_k4_* 2>/dev/null | wc -l                                                # must be 0
```
Report the three lines. If any `max|diff|` is not 0 or `NA-same` is FALSE, **stop and describe**: that would mean
the rewrite is not lossless.

#### K5 — private contact list for the groups we do not hash
```bash
python3 -I - "$P" "$CL" "$W" <<'PY'
import sys, csv, collections, os
p, cl, w = sys.argv[1], sys.argv[2].rstrip("/") + "/", sys.argv[3].rstrip("/") + "/"
tag = {r["owner"]: r["tag"] for r in csv.DictReader(open(os.path.join(p, "owner_usage.tsv")), delimiter="\t")}
rows, pub = [], collections.defaultdict(lambda: [0, 0.0])
for fn, kind in (("dup_trees.tsv", "tree"), ("dup_files.tsv", "file")):
    g = collections.defaultdict(list)
    for r in csv.DictReader(open(os.path.join(p, fn)), delimiter="\t"):
        key = (r.get("name", ""), r.get("bytes_each") or r.get("size"), r["recoverable_bytes"])
        g[key].append(r)
    for key, rs in g.items():
        ds = [r.get("dir") or r.get("path") for r in rs]
        if all(d.startswith(cl) for d in ds) or all(d.startswith(w) for d in ds):
            continue                                   # ours or shared-climate: K2 handles them
        owners = sorted({r["owner"] for r in rs})
        for o in owners:
            pub[tag.get(o, "?")][0] += 1; pub[tag.get(o, "?")][1] += int(key[2]) / 1e9
        for r, d in zip(rs, ds):
            rows.append([kind, key[0], key[1], key[2], d, r["owner"], tag.get(r["owner"], "?")])
with open("logs/dup_k5_private_contacts_" + os.environ["S"] + ".tsv", "w") as o:
    o.write("kind\tname\tbytes_each\trecoverable\tpath\towner\ttag\n")
    for r in sorted(rows, key=lambda r: (r[5], -int(r[3]))): o.write("\t".join(map(str, r)) + "\n")
for t, (n, gb) in sorted(pub.items(), key=lambda kv: -kv[1][1]):
    print(f"{t}\t{n} groups\t{gb:.2f} GB")
PY
```
Report only the printed per-tag lines (a group shared by two owners is counted under both). The private file stays on
the node for Pete. **Privacy check before you paste anything:** run P15's
`grep -c -F -f logs/p15_names_$S.txt` over your RESPONSE text; it must be 0.

#### K6 — report
At the top: our area by owner tag (K1); confirmed duplicates in `$CL` and in our own data, with bytes (K2); how much of our own data is
uncompressed and the compression ratio measured (K3, K4); groups per other owner tag (K5); and the privacy count.

**Stop and describe** on any of: not on `develop`; `logs/dup_*` not ignored; K2 over its 100 GB bound; a lossy K4;
a non-zero privacy count; any write outside `logs/`; anything that would need reading another user's files outside `$CL`, a deletion
outside `logs/dup_k4_*`, or a move.

**To answer:** prepend one `### RESPONSE — pascal [M-20261119-i58dup-1]` to **this file**, run the IP grep over the
response, commit only this file (never `logs/`), `git pull --rebase` before pushing, push, verify it landed, and end
with `Answers: M-20261119-i58dup-1 (complete)` — or `(stopped at <gate>)`.
