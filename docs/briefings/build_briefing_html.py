"""Render a briefing markdown file to a self-contained HTML page.

Usage: python3 docs/briefings/build_briefing_html.py <in.md> <out.html> "<page title>"
Needs `npx marked@14` (Node) for the markdown -> HTML step.
"""
import html, os, re, subprocess, sys, tempfile

src, out, title = sys.argv[1], sys.argv[2], sys.argv[3]
# write via -o: marked's stdout is truncated at ~64 KB when captured through npx
with tempfile.TemporaryDirectory() as td:
    tmp = os.path.join(td, "body.html")
    subprocess.run(["npx", "-y", "marked@14", "--gfm", "-i", src, "-o", tmp], check=True)
    b = open(tmp, encoding="utf-8").read()

m = re.match(r"\s*<h1>(.*?)</h1>\s*(<p>.*?</p>)", b, re.S)
h1, meta, b = m.group(1), m.group(2), b[m.end():]
b = re.sub(r"^\s*<hr>", "", b)

toc = []
def slug(t): return re.sub(r"[^a-z0-9]+", "-", re.sub("<.*?>", "", t).lower()).strip("-")[:48]
def head(mo):
    lvl, txt = mo.group(1), mo.group(2); sid = slug(txt)
    if lvl == "2": toc.append((sid, html.unescape(re.sub("<.*?>", "", txt))))
    return f'<h{lvl} id="{sid}">{txt}</h{lvl}>'
b = re.sub(r"<h([23])>(.*?)</h\1>", head, b, flags=re.S)
b = b.replace("<table>", '<div class="tw"><table>').replace("</table>", "</table></div>")
b = b.replace("<pre>", '<div class="cw"><pre>').replace("</pre>", "</pre></div>")

items, cur = [], ""
for r in [x.strip() for x in meta.replace("<p>", "").replace("</p>", "").split("\n") if x.strip()]:
    if r.startswith("<strong>") and cur: items.append(cur); cur = r
    else: cur = (cur + " " + r).strip()
items.append(cur)
dl = "".join(f'<div class="m"><dt>{re.match(r"<strong>(.*?):</strong>", i).group(1)}</dt><dd>{re.sub(r"<strong>.*?</strong>\s*", "", i, count=1)}</dd></div>' for i in items)
tocl = "".join(f'<li><a href="#{i}">{html.escape(t)}</a></li>' for i, t in toc)

DARK = "--bg:#111614;--surface:#18201d;--ink:#e2eae6;--muted:#9aa8a3;--rule:#2c3733;--accent:#77bdb2;--accent-soft:#1d2f2b;--note:#e0b066;--note-soft:#2a2216;--code-bg:#1f2825"
css = f"""
:root{{--bg:#f6f8f6;--surface:#fff;--ink:#1b2422;--muted:#56645f;--rule:#d6ddd9;--accent:#1d5c55;--accent-soft:#e3efec;--note:#8a5a10;--note-soft:#f8efdf;--code-bg:#eef2f0;
--sans:"IBM Plex Sans",system-ui,-apple-system,"Segoe UI",sans-serif;--cond:"IBM Plex Sans Condensed","Arial Narrow",system-ui,sans-serif;--mono:"IBM Plex Mono",ui-monospace,Menlo,Consolas,monospace}}
@media (prefers-color-scheme:dark){{:root{{color-scheme:dark;{DARK}}}}}
*{{box-sizing:border-box}} html{{-webkit-text-size-adjust:100%}}
body{{margin:0;background:var(--bg);color:var(--ink);font:15.5px/1.62 var(--sans);padding-inline:16px;padding-block:0 64px}}
.wrap{{max-width:1180px;margin:0 auto;display:grid;grid-template-columns:minmax(0,1fr);gap:40px}}
@media (min-width:1000px){{.wrap{{grid-template-columns:240px minmax(0,1fr)}}}}
header.top{{max-width:1180px;margin:0 auto 32px;padding-block:40px 28px;border-bottom:1px solid var(--rule)}}
.eyebrow{{font:600 12px/1.2 var(--cond);letter-spacing:.09em;text-transform:uppercase;color:var(--accent)}}
h1{{font:600 clamp(28px,4.2vw,40px)/1.12 var(--cond);margin:10px 0 18px;text-wrap:balance}}
dl.meta{{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px 28px;margin:0}}
.m dt{{font:600 11.5px/1.3 var(--cond);letter-spacing:.07em;text-transform:uppercase;color:var(--muted)}} .m dd{{margin:3px 0 0;font-size:14px}}
nav.toc{{font-size:13.5px}}
@media (min-width:1000px){{nav.toc{{position:sticky;top:20px;align-self:start;max-height:calc(100vh - 40px);overflow:auto}}}}
nav.toc h2{{font:600 11.5px/1.3 var(--cond);letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin:0 0 10px;border:0;padding:0}}
nav.toc ol{{list-style:none;margin:0;padding:0;display:grid;gap:2px}}
nav.toc a{{display:block;padding:5px 10px;border-left:2px solid var(--rule);color:var(--ink);text-decoration:none}}
nav.toc a:hover,nav.toc a:focus-visible{{border-left-color:var(--accent);color:var(--accent);background:var(--accent-soft)}}
main{{min-width:0;max-width:80ch}}
main h2{{font:600 25px/1.2 var(--cond);margin:52px 0 14px;padding-top:14px;border-top:2px solid var(--ink);text-wrap:balance;scroll-margin-top:20px}}
main h2:first-child{{margin-top:0}}
main h3{{font:600 18px/1.3 var(--cond);margin:32px 0 10px;color:var(--accent);scroll-margin-top:20px}}
p,ul,ol{{margin:0 0 14px}} ul,ol{{padding-left:1.3em}} li{{margin:4px 0}}
a{{color:var(--accent);text-underline-offset:2px}} strong{{font-weight:600}}
code{{font:400 .86em/1.4 var(--mono);background:var(--code-bg);padding:.08em .35em;border-radius:3px;overflow-wrap:anywhere}}
.cw{{overflow-x:auto;margin:0 0 16px;background:var(--code-bg);border:1px solid var(--rule);border-radius:4px}}
pre{{margin:0;padding:14px 16px;font:13px/1.55 var(--mono)}} pre code{{background:none;padding:0;overflow-wrap:normal;white-space:pre}}
.tw{{overflow-x:auto;margin:6px 0 20px;border:1px solid var(--rule);border-radius:4px;background:var(--surface)}}
table{{border-collapse:collapse;width:100%;font-size:13.5px;font-variant-numeric:tabular-nums}}
th{{font:600 12px/1.3 var(--cond);letter-spacing:.05em;text-transform:uppercase;color:var(--muted);text-align:left;background:var(--accent-soft);padding:9px 12px;border-bottom:1px solid var(--rule);white-space:nowrap}}
td{{padding:8px 12px;border-bottom:1px solid var(--rule);vertical-align:top}} tr:last-child td{{border-bottom:0}}
blockquote{{margin:16px 0 20px;padding:12px 16px;background:var(--note-soft);border-left:3px solid var(--note);border-radius:0 4px 4px 0}}
blockquote p:last-child{{margin-bottom:0}}
hr{{border:0;border-top:1px solid var(--rule);margin:36px 0}}
"""
page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)}</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans+Condensed:wght@500;600&family=IBM+Plex+Sans:ital,wght@0,400;0,500;0,600;1,400&display=swap">
<style>{css}</style></head>
<body>
<header class="top"><div class="eyebrow">Africa Agriculture Adaptation Atlas · hazards pipeline briefing · generated from {html.escape(src)}</div>
<h1>{h1}</h1><dl class="meta">{dl}</dl></header>
<div class="wrap"><nav class="toc" aria-label="Contents"><h2>Contents</h2><ol>{tocl}</ol></nav>
<main>{b}</main></div>
</body></html>
"""
open(out, "w").write(page)
print(f"wrote {out}: {len(page)} bytes, {len(toc)} sections")
