#!/usr/bin/env python3
"""storage_census.py - metadata-only duplicate census of a shared store (issue #58).

Reads a file listing made with GNU find (no file contents are read, by this
script or by the listing):

    find "$B" -xdev -type f -printf '%s\\t%u\\t%h\\t%f\\n' > files.tsv

and reports, for the whole store:

  * usage per owner and per top-level location;
  * PROBABLE duplicate directory trees: two trees whose files have the same
    names and sizes all the way down (a Merkle signature over name + size);
    only the largest duplicated tree is reported, not every sub-folder in it;
  * PROBABLE duplicate large files outside those trees: same name, same size,
    two or more places.

"Probable" because names and sizes match but contents were not compared.
Folders of fixed-size files that reuse names (e.g. <year>/day_001.tif written
uncompressed) can match without being copies; check a group's paths before acting.
The owner shown for a tree is the owner of its largest file.
Confirming a group (md5 of the copies) is a separate, deliberate step.

Two outputs, kept apart on purpose:

  --private DIR   full detail WITH usernames and real paths. Stays on the host;
                  it is what a person uses to contact owners. Never commit it.
  --public FILE   a markdown summary for a public repository: usernames are
                  replaced by owner-NN tags (our own account by "self"), the
                  root by $B, and any path component that contains a username
                  is replaced by that owner's tag.

Usage:
    python3 -I scripts/storage_census.py --list files.tsv --root "$B" \\
        --self "$USER" --private logs/p15_private --public logs/p15_public.md \\
        [--min-tree-gb 1] [--min-file-mb 100] [--top 40]

Standard library only, so it runs under any python3 >= 3.8.
"""
import argparse
import collections
import hashlib
import os
import re
import sys


def human(n):
    for unit, div in (("TB", 1e12), ("GB", 1e9), ("MB", 1e6)):
        if n >= div:
            return f"{n / div:.2f} {unit}"
    return f"{n:.0f} B"


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--list", required=True)
    ap.add_argument("--root", required=True)
    ap.add_argument("--self", dest="me", default="")
    ap.add_argument("--private", required=True)
    ap.add_argument("--public", required=True)
    ap.add_argument("--min-tree-gb", type=float, default=1.0)
    ap.add_argument("--min-file-mb", type=float, default=100.0)
    ap.add_argument("--top", type=int, default=40)
    a = ap.parse_args()
    root = a.root.rstrip("/")
    os.makedirs(a.private, exist_ok=True)

    # ---- read the listing -------------------------------------------------
    files = collections.defaultdict(list)          # dir -> [(name, size)]
    owner_of_dir = {}                              # dir -> owner of its first file
    big = {}                                       # dir -> (largest size, its owner)
    by_owner = collections.Counter()
    n_owner = collections.Counter()
    n_files = total = bad = 0
    with open(a.list, encoding="utf-8", errors="surrogateescape") as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 4 or not parts[0].isdigit():
                bad += 1
                continue
            size, owner, d, name = int(parts[0]), parts[1], parts[2].rstrip("/"), parts[3]
            files[d].append((name, size))
            owner_of_dir.setdefault(d, owner)
            if size >= big.get(d, (-1, ""))[0]:
                big[d] = (size, owner)
            by_owner[owner] += size
            n_owner[owner] += 1
            n_files += 1
            total += size
    if not n_files:
        sys.exit("no rows read from " + a.list)

    # ---- pseudonymise ------------------------------------------------------
    ranked = [o for o, _ in by_owner.most_common()]
    tag = {}
    k = 0
    for o in ranked:
        if a.me and o == a.me:
            tag[o] = "self"
        else:
            k += 1
            tag[o] = f"owner-{k:02d}"
    # longest names first so "ab" does not eat part of "abc"
    names = sorted((o for o in tag if len(o) >= 3), key=len, reverse=True)
    name_re = re.compile("|".join(re.escape(o) for o in names)) if names else None

    def pub(path):
        p = path
        if p == root:
            p = "$B"
        elif p.startswith(root + "/"):
            p = "$B/" + p[len(root) + 1:]
        if name_re:
            p = name_re.sub(lambda m: tag[m.group(0)], p)
        return p

    # ---- tree signatures, deepest directory first -------------------------
    children = collections.defaultdict(set)
    all_dirs = set(files)
    for d in list(files):
        cur = d
        while cur.startswith(root + "/"):
            parent = os.path.dirname(cur)
            children[parent].add(cur)
            if parent in all_dirs:
                break
            all_dirs.add(parent)
            cur = parent
    sig, tbytes, tfiles, towner = {}, {}, {}, {}
    for d in sorted(all_dirs, key=lambda x: x.count("/"), reverse=True):
        h = hashlib.sha1()
        b = n = 0
        best = big.get(d, (-1, "?"))
        for name, size in sorted(files.get(d, ())):
            h.update(f"f\0{name}\0{size}\n".encode("utf-8", "surrogateescape"))
            b += size
            n += 1
        for c in sorted(children.get(d, ())):
            h.update(f"d\0{os.path.basename(c)}\0{sig[c]}\n".encode("utf-8", "surrogateescape"))
            b += tbytes[c]
            n += tfiles[c]
            if towner[c][0] > best[0]:
                best = towner[c]
        sig[d], tbytes[d], tfiles[d], towner[d] = h.hexdigest(), b, n, best

    groups = collections.defaultdict(list)
    min_tree = a.min_tree_gb * 1e9
    for d, s in sig.items():
        if tbytes[d] >= min_tree and tfiles[d] > 0:
            groups[s].append(d)
    groups = {s: sorted(ds) for s, ds in groups.items() if len(ds) > 1}
    # Keep only the outermost copies: walk groups from the shallowest member down and
    # drop any member that already sits inside a kept copy, so a duplicated tree is
    # counted once and not again for each sub-folder (or for repeats within itself).
    kept_dirs = set()

    def inside_kept(d):
        cur = os.path.dirname(d)
        while cur.startswith(root):
            if cur in kept_dirs:
                return True
            if cur == root:
                return False
            cur = os.path.dirname(cur)
        return False

    maximal = {}
    for s_, ds in sorted(groups.items(), key=lambda kv: min(x.count("/") for x in kv[1])):
        keep = [d for d in ds if not inside_kept(d)]
        if len(keep) > 1:
            maximal[s_] = keep
            kept_dirs.update(keep)
    covered = kept_dirs

    def in_covered(d):
        cur = d
        while cur.startswith(root):
            if cur in covered:
                return True
            if cur == root:
                return False
            cur = os.path.dirname(cur)
        return False

    tree_rows = sorted(((len(ds) - 1) * tbytes[ds[0]], s, ds) for s, ds in maximal.items())[::-1]

    # ---- large files outside the duplicate trees --------------------------
    min_file = a.min_file_mb * 1e6
    fgroups = collections.defaultdict(list)
    for d, lst in files.items():
        for name, size in lst:
            if size >= min_file:
                fgroups[(name, size)].append(d)
    file_rows = []
    for (name, size), ds in fgroups.items():
        ds = [d for d in ds if not in_covered(d)]
        if len(ds) > 1:
            file_rows.append(((len(ds) - 1) * size, name, size, sorted(ds)))
    file_rows.sort(reverse=True)

    # ---- top-level usage ---------------------------------------------------
    by_top = collections.Counter()
    for d, lst in files.items():
        rel = d[len(root) + 1:] if d.startswith(root + "/") else ""
        top = "/".join(rel.split("/")[:2]) if rel else "."
        by_top[top] += sum(s for _, s in lst)

    # ---- private outputs (usernames, real paths) --------------------------
    with open(os.path.join(a.private, "owner_usage.tsv"), "w") as o:
        o.write("owner\ttag\tfiles\tbytes\n")
        for ow in ranked:
            o.write(f"{ow}\t{tag[ow]}\t{n_owner[ow]}\t{by_owner[ow]}\n")
    with open(os.path.join(a.private, "dup_trees.tsv"), "w") as o:
        o.write("recoverable_bytes\tcopies\tbytes_each\tfiles_each\tpath\towner\n")
        for rec, s, ds in tree_rows:
            for d in ds:
                o.write(f"{rec}\t{len(ds)}\t{tbytes[d]}\t{tfiles[d]}\t{d}\t{towner[d][1]}\n")
    with open(os.path.join(a.private, "dup_files.tsv"), "w") as o:
        o.write("recoverable_bytes\tcopies\tsize\tname\tdir\towner\n")
        for rec, name, size, ds in file_rows:
            for d in ds:
                o.write(f"{rec}\t{len(ds)}\t{size}\t{name}\t{d}\t{owner_of_dir.get(d, '?')}\n")

    # ---- public summary ----------------------------------------------------
    rec_t = sum(r for r, _, _ in tree_rows)
    rec_f = sum(r[0] for r in file_rows)
    L = []
    L.append(f"| Files listed | {n_files:,} |")
    L.append(f"| Bytes listed | {human(total)} |")
    L.append(f"| Unparsed lines | {bad} |")
    L.append(f"| Owners | {len(ranked)} |")
    L.append(f"| Probable duplicate trees (>= {a.min_tree_gb:g} GB, maximal) | {len(tree_rows)} groups, {human(rec_t)} recoverable |")
    L.append(f"| Probable duplicate large files outside those trees (>= {a.min_file_mb:g} MB) | {len(file_rows)} groups, {human(rec_f)} recoverable |")
    out = ["#### Summary", "", "| | |", "|---|---:|"] + L + [""]
    out += ["#### Usage by owner (top %d)" % a.top, "", "| Owner | Files | Size | Share |", "|---|---:|---:|---:|"]
    for ow in ranked[: a.top]:
        out.append(f"| {tag[ow]} | {n_owner[ow]:,} | {human(by_owner[ow])} | {100 * by_owner[ow] / total:.1f} % |")
    out += ["", "#### Usage by location, two levels (top %d)" % a.top, "", "| Location | Size |", "|---|---:|"]
    for top, b in by_top.most_common(a.top):
        out.append(f"| {pub(root + '/' + top) if top != '.' else '$B'} | {human(b)} |")
    out += ["", "#### Probable duplicate trees, largest first (top %d)" % a.top, "",
            "| # | Recoverable | Copies | Each | Files each | Copies at (owner) |", "|---:|---:|---:|---:|---:|---|"]
    for i, (rec, s, ds) in enumerate(tree_rows[: a.top], 1):
        where = "<br>".join(f"`{pub(d)}` ({tag.get(towner[d][1], '?')})" for d in ds)
        out.append(f"| {i} | {human(rec)} | {len(ds)} | {human(tbytes[ds[0]])} | {tfiles[ds[0]]:,} | {where} |")
    out += ["", "#### Probable duplicate large files outside those trees (top %d)" % a.top, "",
            "| # | Recoverable | Copies | Size | Name | Copies at (owner) |", "|---:|---:|---:|---:|---|---|"]
    for i, (rec, name, size, ds) in enumerate(file_rows[: a.top], 1):
        where = "<br>".join(f"`{pub(d)}` ({tag.get(owner_of_dir.get(d), '?')})" for d in ds)
        out.append(f"| {i} | {human(rec)} | {len(ds)} | {human(size)} | `{pub(name)}` | {where} |")
    with open(a.public, "w") as o:
        o.write("\n".join(out) + "\n")
    print(f"listed {n_files:,} files, {human(total)}; {len(tree_rows)} duplicate-tree groups ({human(rec_t)}), "
          f"{len(file_rows)} duplicate-file groups ({human(rec_f)}); private -> {a.private}, public -> {a.public}")


if __name__ == "__main__":
    main()
