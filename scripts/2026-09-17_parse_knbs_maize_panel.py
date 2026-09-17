import re, sys, csv
sys.path.insert(0,'/Users/pstewarda/Documents/rprojects/hazards_prototype/python')
from _knbs_admin import resolve_county, norm_name

ROW = re.compile(r"^\s*([A-Za-z'’\.\- ]+?)\s+((?:[\d,]+\s+){9}[\d,]+)\s*$")

def parse(path, years):
    txt = open(path, errors="replace").read().splitlines()
    # the first "Annex 1:" hit is the table of contents; the real annex is further down and is
    # split across pages, each repeating the heading
    heads = [i for i,l in enumerate(txt) if l.strip().startswith("Annex 1:")]
    start = heads[1] if len(heads) > 1 else heads[0]
    end   = next((i for i,l in enumerate(txt) if i>start and l.strip().startswith("Annex 2:")), len(txt))
    out = {}
    for line in txt[start:end]:
        m = ROW.match(line)
        if not m: continue
        name = m.group(1).strip()
        if name.lower() in ("county","total","kenya"): continue
        nums = [int(x.replace(",","")) for x in m.group(2).split()]
        try: pcode, codab = resolve_county(name)
        except KeyError:
            print("  UNRESOLVED county:", repr(name), file=sys.stderr); continue
        for i, yr in enumerate(years):
            out[(pcode, yr)] = dict(adm1_pcode=pcode, adm1_name=codab, year=yr,
                                    area_ha=nums[2*i], production_t=nums[2*i+1])
    return out

a = parse("napr2024.txt", [2019,2020,2021,2022,2023])
b = parse("napr2025.txt", [2020,2021,2022,2023,2024])
print(f"napr2024: {len(a)} county-years | napr2025: {len(b)} county-years", file=sys.stderr)

# overlap check: do the two reports agree where they overlap?
ov = [(k, a[k]['production_t'], b[k]['production_t']) for k in set(a)&set(b)]
diff = [(k,x,y) for k,x,y in ov if x!=y]
print(f"overlap {len(ov)} rows, disagreeing {len(diff)}", file=sys.stderr)
for k,x,y in sorted(diff, key=lambda t:-abs(t[1]-t[2]))[:5]:
    print(f"   {k}: 2024rpt={x:,} 2025rpt={y:,}", file=sys.stderr)

merged = dict(a); merged.update(b)          # later report wins on overlap (revised figures)
rows = sorted(merged.values(), key=lambda r:(r['adm1_pcode'], r['year']))
w = csv.DictWriter(open("maize_panel.csv","w",newline=""), fieldnames=["adm1_pcode","adm1_name","year","area_ha","production_t"])
w.writeheader(); w.writerows(rows)
yrs = sorted({r['year'] for r in rows}); cs = sorted({r['adm1_pcode'] for r in rows})
print(f"PANEL: {len(rows)} rows | {len(cs)} counties | years {yrs}", file=sys.stderr)
for y in yrs:
    tot = sum(r['production_t'] for r in rows if r['year']==y)
    print(f"   {y}: national {tot:,} t", file=sys.stderr)
