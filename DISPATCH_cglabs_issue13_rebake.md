# DISPATCH — cglabs — #13: the full R/2 + R/3 rebake, and the five-tier publish

**Append-only; newest block on top. Prepend a `### RESPONSE` block to answer.**

### RESPONSE cglabs 2026-10-19 (c) — Step 3 (`ha`) STOPS at G6b row 1: six UNNAMED pairs, all tiny areas. The gate's materiality floor is unit-blind. Nothing written.

Step 2's RESPONSE is in the block below. Step 3 dry-run only; **`ha` FAILs row 1 on all three
tiers, nothing published, steps 4-5 not started, no `--allow-exposure-mismatch`.**

Your two `ha` expectations held: the **declared UNMODELLED block appears** (450 pairs, 0.03 B ha —
the combined reference does carry the composites), and **TZA:coconut is named** at 0.9761.

## The breach

```
1: material pairs within tol   max |ratio-1| = 0.0733 over 757 pairs (+1 NAMED residual(s) allowed)   FALSE
2: no product pair invents value without exposure                                      0 pair(s)      TRUE
3: no material exposure pair missing from the product                                  0 pair(s)      TRUE
4: product has the full scenario set                             historic,...,ssp585                  TRUE
```

**Six unnamed pairs breach 0.02, and every one of them is a tiny area:**

```
     iso3    crop        E      T     ratio
1:    COD  barley   1395.3   1293 0.9267
2:    MDG     tea   1018.5    955 0.9377
3:    MDG  lentil   1232.5   1199 0.9728
4:    MDG sorghum   2373.7   2314 0.9748
5:    TZA    yams   1476.3   1441 0.9761
6:    COD tobacco   8213.1   8025 0.9771
```

(a seventh row, TZA:coconut at 172,681 ha, is the named Wete residual — the gate excluded it; my
probe did not, which is why it shows up in my own table.)

## The deviation is a function of area, and the whole population says so

758 material pairs, **median deviation 0.000126**:

```
        bin pairs     max_dev   median_dev n_over_002
1:    1k-2k    57 0.073324208 1.809627e-03          4
2:    2k-5k    92 0.025150609 5.850998e-04          1
3:   5k-10k    68 0.022904816 2.813733e-04          1
4: 10k-100k   295 0.004343358 1.477167e-04          0
5:  100k-1M   192 0.023916820 3.000821e-05          1   <- TZA:coconut, the named one
6:      >1M    54 0.001916485 1.749680e-05          0
```

**Set the named pair aside and every pair above 10,000 ha is within 0.44 %.** All six breaches sit
below 8,213 ha, four of them below 2,400 ha.

## Admin2 decomposition, as you asked — and it is NOT the Wete mechanism

```
COD:barley    units with exposure and ZERO product: 33, E_lost 25.2 (1.81% of national)
MDG:tea        0 units,  0.00%      MDG:lentil   0 units, 0.00%
MDG:sorghum    0 units,  0.00%      TZA:yams    25 units, 0.42%
COD:tobacco    4 units,  0.03%
```

Only COD:barley has a meaningful coverage hole, and at 1.81 % it does not account for its 7.33 %.
Three of the six have **no zero-product unit at all**. The shortfall is instead spread thinly across
units that *are* present:

```
MDG:tea      Analanjirofo 107.7 -> 103   Anosy 29.5 -> 25   Melaky 15.4 -> 11
MDG:sorghum  Menabe        93.5 ->  81   Atsimo Andrefana 164.8 -> 154
```

**Measured fact: every product value is an integer** — `T all integers: TRUE`, no fractional value
in 169,316 sampled rows — while the exposure side is fractional. The same is true of the `vop_usd`
tier, so the storage is general, not an `ha` defect. **And it is two-sided: 240 pairs sit above 1
and 518 below, so this is rounding, not truncation.**

**My inference, flagged as inference:** a zone's value is accumulated from pixels that are each
integral, so the absolute error grows with the pixel count while the relative error falls with
area. That is exactly the shape of the table above. I have not opened R/3's write path to confirm
where the integerisation happens, and I would rather you point me at it than guess further.

What this means for the gate: **`min_exposure = 1e3` is unit-blind.** 1,000 USD is genuine noise;
1,000 *hectares* is a real area, so the physical tiers admit a population that currency tiers never
had — 57 pairs between 1k and 2k ha, where one hectare of rounding is 0.1 %.

## Not deciding it

1. **A unit-aware materiality floor** — `min_exposure` per tier, e.g. 1e4 ha for the physical ones.
   Principled: materiality has to scale with the unit, and at 1e4 ha the existing 2 % bound passes
   with 0.44 % to spare. It also leaves the bound itself untouched.
2. **Name the six.** I would avoid it: they are not one mechanism with one reason, and the next
   tiny pair would need naming too. The named-residual file works because Wete is a *fact*.
3. **Loosen `tol_pair` for `ha`.** Blunts the bound for all 757 pairs to admit six.

Option 1 is the one I would build, and it is testable: state the floor, and the claim "every pair
above it is within 2 %" is checkable in one run.

**What I need:** the floor, or another route. `prod_t` and `head_n` are untouched — `prod_t` is in
tonnes and will have the same unit-blindness, `head_n` in animal numbers likely more so. I have not
run them, since the dispatch is one step per command and this step has not passed.


### RESPONSE cglabs 2026-10-19 (b) — Step 2 is LIVE. Row 3 clean, the 62.42 B reported not gated, and the published parquet reads 266.83 B I$.

Pulled `93965df`. Fixture, dry-run, then live. **exit=0, 1.0 min, six keys, no backups — first
publish, so G6 was skipped, and no declaration flags were passed.**

## The dry-run, on all three tiers

```
1: material pairs within tol    max |ratio-1| = 0.0059 over 1350 pairs (+1 NAMED residual(s) allowed)   TRUE
2: no product pair invents value without exposure                                        0 pair(s)      TRUE
3: no material exposure pair missing from the product                                    0 pair(s)      TRUE
4: product has the full scenario set                               historic,...,ssp585                  TRUE

  declared UNMODELLED crops in the exposure (no hazard frequency exists; reported, not gated): 416 pairs, 62.42 B total
                crop pairs           E
1:        vegetables    50 24098643782
2:    tropical-fruit    48 13959045330
3:     rest-of-crops    49  8413332269
4:       other-roots    44  5531650945
5:   temperate-fruit    50  5154205923
6:     other-cereals    41  2445821997
7:      other-pulses    47  1790819158
8:   other-oil-crops    45   945084151
9: other-fibre-crops    42    77692897
```

The block totals **62,416,296,452 I$** — my measured figure to the unit. TZA:coconut named at
0.9759 with its reason; ratios unchanged at 0.0059.

**One number to read correctly:** the block counts **416** pairs where row 3 previously reported
**389**. Row 3 applies `E > min_exposure` and the new block does not, so 27 of the nine groups'
pairs are sub-1,000-I$ noise. Two filters, not a discrepancy — worth knowing before 416 gets quoted
somewhere as the population count.

## What is live

Bucket listing taken after the run, under `.../variable=vop_intld15-21/period=jagermeyr/`:

```
06:31:52     82,761,789  model=ENSEMBLEmean/severity=extreme/int=multi-hazard.parquet
06:31:58         16,753  model=ENSEMBLEmean/severity=extreme/int=multi-hazard.parquet.json
06:31:34     85,345,945  model=ENSEMBLEmean/severity=moderate/int=multi-hazard.parquet
06:31:40         16,874  model=ENSEMBLEmean/severity=moderate/int=multi-hazard.parquet.json
06:31:16     84,809,344  model=ENSEMBLEmean/severity=severe/int=multi-hazard.parquet
06:31:22         16,632  model=ENSEMBLEmean/severity=severe/int=multi-hazard.parquet.json
```

Every size matches local to the byte. `no live object … (first publish)` logged on all three
severities, so nothing was overwritten and no backup was due.

## Read back off the published parquet, as asked

I downloaded the published extreme key from S3 and computed from that, not from the local file:

```
=== continental total OFF THE PUBLISHED PARQUET ===
all pairs 266.83 B I$ | modelled crops 177.81 | livestock 89.02
```

**266.83 B I$ on S3**, matching the local measurement and the 240.24 - 62.42 + 89.02 = 266.84
arithmetic. The three numbers for the CDH record and the Brayden note are therefore **266.83 total /
177.81 modelled crops (vs 177.82 in the twin) / 62.42 unmodelled**, and the consumer fact stands:
**19 % of the recovered constant-dollar exposure sits in groups the hazard tiers cannot see.**

Proceeding to steps 3-4 (`ha`, `prod_t`, `head_n` against the combined reference), one step per
command, dry-run then live, RESPONSE per step. Watching for the unmodelled block on `ha` and
`prod_t` and its absence on `head_n`, and for any UNNAMED breach — including a livestock pair on
`head_n`, which the residuals file does not cover.


### MACBOOK 2026-10-19 (b) — option 1, built. And my 240.24 expectation was wrong; yours is the right accounting. Re-run step 2, then 3-5.

**Row 3 did its job and your option 1 is the right build, for the reason you gave:** option 2 makes
the gate unfalsifiable, option 3 invents an artefact the bake would then have to carry. Shipped:

- **`G6B_NO_PRODUCT_CROPS`** — the nine composite groups, declared with the reason in the file,
  env-overridable like `DRIFT_NO_REF`. Row 3 excludes them and reports them separately, per crop with
  their E totals, so the 62.42 B is *visible* in every gate run rather than silently waved past.
- **An undeclared missing crop still fails.** The fixture pins all three behaviours: a declared
  composite is reported-not-failed; the same pair fails without the declaration; and a REAL crop with
  exposure and no product row fails regardless — a truncated tier cannot hide behind the composites.

**On 240.24: I was wrong and your decomposition is the correct accounting.** The B5 headline is the
twin's crop side; the tier includes livestock and cannot include the composites, and your
240.24 − 62.42 + 89.02 = 266.84 against the product's 266.83 closes it. For the CDH record and the
Brayden note the statement will be all three numbers, not one:

- tier total (all pairs) **266.83 B I$**;
- modelled-crops-only **177.81 B** against a twin reference of 177.82;
- the twin headline 240.24 B includes **62.42 B in nine composite groups that carry exposure but no
  hazard product**, because they are aggregates with no hazard frequency — stated as a property of
  the product, not a footnote.

That last line is a real consumer fact your stop surfaced: **19 % of the recovered constant-dollar
exposure is in groups the hazard tiers cannot see.** It goes to Brayden with the key list.

**Re-run step 2 after `git pull`** (expect this commit or later): dry-run — expect row 3 ok with the
nine groups in the new `declared UNMODELLED` block at ~62.42 B, TZA:coconut named as before, ratios
unchanged — then live. Then steps 3-5 as already cleared. `ha` and `prod_t` gate against the combined
reference, which carries the composites too, so expect the same unmodelled block there; `head_n`
filters `exposure == "number"` and should not.

---

### RESPONSE cglabs 2026-10-19 (a) — Step 2 dry-run STOPS at G6b. Not a ratio breach: the intld twin carries nine composite crop groups the hazard product cannot model. And 240.24 B is the crop side only — the arithmetic closes.

Pulled `06f400c`. Correction taken — no `--hv-retired` / `--hv-added` passed, and G6 was indeed
skipped (first publish at `variable=vop_intld15-21`). **Step 2 dry-run FAILs G6b on all three tiers,
so nothing was written and steps 3-5 are not started.** No `--allow-exposure-mismatch`.

## The ratios are fine; the breach is row 3

```
1: material pairs within tol    max |ratio-1| = 0.0059 over 1350 pairs (+1 NAMED residual(s) allowed)   TRUE
2: no product pair invents value without exposure                                        0 pair(s)      TRUE
3: no material exposure pair missing from the product                                  389 pair(s)      FALSE
4: product has the full scenario set                               historic,...,ssp585                  TRUE
```

**Your TZA:coconut prediction holds exactly** — it appears in the NAMED block with its reason, at
0.9759 (E 78,746,552, T 76,850,530). The rest of the material population's worst is 0.0059. So the
tier reproduces its exposure wherever it has rows at all.

## What the 389 pairs are

```
intld twin crops: 52  |  usd twin crops: 43  |  product crops: 44

crops in the INTLD twin but NOT in the product          E (I$)
   vegetables                                      24,098,643,782
   tropical-fruit                                  13,959,045,330
   rest-of-crops                                    8,413,332,269
   other-roots                                      5,531,650,945
   temperate-fruit                                  5,154,205,923
   other-cereals                                    2,445,821,997
   other-pulses                                     1,790,819,158
   other-oil-crops                                    945,084,151
   other-fibre-crops                                   77,692,897
                                            total  62,416,296,452

crops in the USD twin but NOT in the product:  (empty)
```

389 = those nine SPAM composite groups across the countries that carry them. They are aggregates,
not modelled crops — R/3 has no hazard frequency for `rest-of-crops`, so the product has no row to
multiply. **This is why step 1 passed this gate with 0 and step 2 cannot:** the usd twin has 43
crops and none of these; the intld twin has 52 and all nine. They are the same nine groups B5
*recovered* — `archive/dispatches/DISPATCH_cglabs_b5_fao_code_join.md:289` lists vege +13.06,
trof +7.38, rest +8.41 (was absent), orts +5.53 as the bulk of the +20 %.

## Your 240.24 B expectation — it is the crop side only, and the arithmetic closes

I could not read it off the published parquet, because nothing published. Off the local tier and its
twin, admin2 basis:

```
intld twin, all crops + livestock : 329.26 B I$
      minus livestock             : 240.24 B I$   <- the B5 number, exactly
      minus the nine composites   : 177.82 B I$
      plus livestock back         : 266.84 B I$   <- what the product should read

product (local intld tier, admin0 any+none, max over hazard_vars):
      all pairs 266.83 | crops-only 177.81 | livestock 89.02 | generic-crop 0.00
```

**240.24 B is the twin's crop side excluding livestock.** The product cannot read it, for two
reasons pulling opposite ways: it is missing the nine composites (-62.42) and it includes livestock
(+89.02). 240.24 - 62.42 + 89.02 = **266.84**, and the product reads **266.83**. The +20 % has
reached the tier; it is simply measured over a different set than the reference headline.

If you want the published number to be comparable to 240.24, the quantity is the tier's crops-only
total, **177.81 B I$**, against a reference of 177.82 — and the gap to 240.24 is exactly the
composite groups the hazard layer does not model.

## Not deciding this

Row 3 is a real gate doing its job: an exposure pair with no product row *could* be a silently
truncated tier, which is what it was written to catch. Here it is nine crop groups that can never
have a product row. As I see it:

1. **A declared no-product crop list** — the `DRIFT_NO_REF` pattern, which already exempts
   `generic-crop` from the ratio population, extended to row 3 for the nine composites. Names them,
   says why, and a *new* missing crop still fails. This is the one I would build.
2. **Filter the twin to the product's crop set** before gating. Simpler, but it makes row 3
   unfalsifiable — a genuinely truncated tier would also vanish from the comparison.
3. **Gate intld against a composite-free twin.** Cleanest conceptually, but it is a new exposure
   artefact for the bake to carry, and the twin is what B5 published.

Option 1 keeps row 3 able to catch the thing it exists for. Option 2 is the one I would avoid.

**What I need:** your call on row 3, and whether the 240.24 expectation should be restated as
177.81 B crops-only (or 266.83 all-pairs) for the CDH record. Steps 3-5 are untouched; `ha`,
`prod_t` and `head_n` gate against the combined reference and may or may not hit the same thing — I
have not run them, because the dispatch is one step per command and this step has not passed.


### MACBOOK 2026-10-19 — step 1 accepted. One correction to your point 1, then GO steps 2-5.

**Step 1 is accepted in full.** The declaration closing verbatim on all three tiers, the bucket
listing verified independently of the publisher's own report, and the backups re-counted after the
run — nothing to add. The usd tiers of the #13 rebake are live.

**Correction to your point 1: `vop_intld` has NO live object at the key the publisher gates
against.** The live constant-dollar product sits on the legacy
`source=atlas_cmip6/.../variable=vop_intld15` route, which is retired and is a *different key*; G6
checks whatever is at its own target key, `variable=vop_intld15-21`, which has never existed. So
step 2 is a first publish: **G6 is skipped, G6b with the named residual is the gate, and the
declaration is inert on every remaining step.**

**Do not pass `--hv-retired` / `--hv-added` on steps 2-5.** Not even for the record: a flag that does
nothing teaches the next reader that it is needed, and the gate's meaning stays cleaner if the
declaration appears exactly where a live object exists to drift against — which in this bake was
step 1 alone.

**Your point 2, adjusted for the same reason:** annual (step 5) is also a first publish — the key
has never carried `period=annual` — so no G6, no declared-change line, no row arithmetic against a
live object. The gates there are G1-G4 and G6b. Dry-run first and report, as you intended; expect
the G6b table, not the parity table.

**GO: steps 2-4 (jagermeyr), then step 5 (annual), one step per command as the dispatch stands.**
Dry-run then live for each; prepend a RESPONSE per step rather than at the end. Bases as the
dispatch lists them — intld takes its own res-25 twin, the three physical tiers take the combined
reference with their own `exposure ==` values (`vop` is told apart by the twin FILE for intld).

**Expectations to hold:**
- **TZA pairs on the other tiers:** the Wete hole is in the hazard layer, so expect the named
  TZA:coconut line again on intld and prod_t (same SPAM production share). **If an UNNAMED pair
  breaches on any tier — including a livestock pair on `head_n`, which the file does not cover —
  stop and report with the admin2 decomposition, exactly as you did for Wete.** A second named row
  needs its own evidence, not an extension of the first.
- **intld magnitude:** the continental total off the published parquet should read **240.24 B I$**
  against its twin — the B5 number. That is the first time the +20 % reaches a hazard tier on S3.
- **head_n / prod_t / ha:** G6b ratios should sit at 1 within rounding; these are the identity by
  construction, so their G6b is the first real test of the physical tiers end to end.

After step 5: B6 (post-publish probes, the self-contained any+none check against
`crop-livestock_all_res-25`), then this thread closes into the CDH records and the Brayden note,
which are macbook work.

---

### RESPONSE cglabs 2026-10-16 (e) — B5 step 1 is LIVE. All gates PASS on all three tiers. Stopped for your read before steps 2-5.

Pulled `eff82b6`. Fixture, dry-run with both declaration flags, then the same command live.
**exit=0, 2.3 min, six keys published, three backups written.** No forbidden flag was passed at any
point.

## The declaration closes, verbatim

```
[gate-drift] declared hazard_vars change: retiring [PTOT-L+NTxS+PTOT-G] (25,775,145 live rows),
             adding [PTOT-L+NTxM+PTOT-G,NDWS+NTxM+NDWL0] (51,550,290 local rows)
  G6 value drift vop_usd/{severe,moderate,extreme}: PASS
 1: parity  rows         ok   live 67952655 - retired + added = 93727800 vs local 93727800 (declared change closes exactly)
 2: parity  hazard_vars  ok   live 4 vs local 5 distinct (declared change applied)
 3: parity  scenarios    ok
```

Identical on all three tiers. The fixture's new assertion — *"an INCOMPLETE declaration still FAILs
— the gate checks the stated change, not the presence of a statement"* — passes, so the gate is
checking our arithmetic rather than our intent.

G6b, also on all three: `max |ratio-1| = 0.0057 over 1359 pairs (+1 NAMED residual(s) allowed)`,
TZA:coconut reported with its reason. G1-G5 ok throughout; G5 17 columns identical to live.

## What is now live

Verified by listing the bucket after the run, not from the publisher's own report:

```
18:22:26     84,569,129  .../model=ENSEMBLEmean/severity=extreme/int=multi-hazard.parquet
18:22:33         17,725  .../model=ENSEMBLEmean/severity=extreme/int=multi-hazard.parquet.json
18:21:45     86,767,465  .../model=ENSEMBLEmean/severity=moderate/int=multi-hazard.parquet
18:21:59         17,846  .../model=ENSEMBLEmean/severity=moderate/int=multi-hazard.parquet.json
18:20:55     86,438,784  .../model=ENSEMBLEmean/severity=severe/int=multi-hazard.parquet
18:21:14         17,604  .../model=ENSEMBLEmean/severity=severe/int=multi-hazard.parquet.json
```

under `domain=hazard_exposure/source=nex-gddp-cmip6/region=ssa/processing=hazard-risk-exposure/`
`variable=vop_nominal-usd21/period=jagermeyr/`. Every size matches the local file to the byte, and
the publisher's own `SIZE MATCH` check agreed on all six.

## Backups

**New:** `sandbox/backup/issue9_20261016_182012/` — 3 keys, 190,786,155 bytes, one per severity,
each written *before* its overwrite.

**Held:** the nine `issue9_20261008_07*` keys are still present — I re-counted them after the run,
and they stay until this bake has published in full and been verified, as instructed.

## Stopping here

Step 1 only, as the dispatch says. Steps 2-5 not started: `vop_intld`, then `ha` / `prod_t` /
`head_n` against `exposure_adm_sum_spam20-20_glw420-20_res-25.parquet`, then the same five on
`--timeframe annual`. `--model ENSEMBLE` and per-GCM remain step 6 and out of this dispatch.

Two things worth your eye before I continue:

1. **The same `--hv-retired` / `--hv-added` declaration will be needed at steps 2-5** — the
   hazard_vars change is in the hazard layer, so it applies to every tier. For the four tiers with
   **no live object**, G6 cannot run at all and the declaration is inert; `vop_intld` is the one
   that has a live object to drift against, so it is the one that needs the flags. Say if you want
   them passed on all of them for the record anyway, and I will.
2. **The annual timeframe will have its own row arithmetic.** I have not measured it. I will dry-run
   and report the declared-change line before any annual publish rather than assume the jagermeyr
   numbers carry.


### MACBOOK 2026-10-16 (d) — your option 2, built. Parity now checks the change we intend. Run step 1: dry, then LIVE.

**Your reading is exactly right, on both halves.** The value populations are the question "did
anything other than #13 move?", and they all hold — totals `[0.9959, 1]`, material median 1,
livestock `[0.9998, 1]`, zero flips, zero unmatched. And a bake whose purpose is to retire `NTxS`
and add the per-crop optimum *cannot* pass strict parity against the pre-#13 object; if it did, THAT
would be the stop. Your row-arithmetic close (93,727,800 − 25,775,145 = 67,952,655, to the row) is
the whole case in one line.

You were also right to refuse `--allow-value-drift`: it is the blunt flag, it would wave through the
value populations that are currently doing their job, and parity is shape, not drift.

**Built: the declaration.** `tier_drift()` gains `hv_retired` / `hv_added`; the publisher takes
`--hv-retired` / `--hv-added`. Parity then checks the declared change **closes** — live sets minus
retired plus added must equal the local sets, and live rows minus the retired sets' rows plus the
added sets' rows must equal the local count, **exactly**. An undeclared difference still fails; an
incomplete declaration still fails; with no declaration the old strict equality applies, so every
archived invocation means what it meant. Fixture-pinned, all three behaviours.

**Run step 1 after `git pull`** (expect this commit or later):

```
Rscript scripts/r3_publish_tiers.R --variables vop_usd \
  --drift-exposure vop_usd=<res-25 usd twin> \
  --hv-retired PTOT-L+NTxS+PTOT-G \
  --hv-added PTOT-L+NTxM+PTOT-G,NDWS+NTxM+NDWL0 \
  --dry-run
```

**Expected:** G6b PASS with the TZA:coconut NAMED line as before; G6 parity rows both ok, the rows
line reading `(declared change closes exactly)` with your 67,952,655 → 93,727,800 arithmetic; every
value population unchanged from today's run. **Then the same command without `--dry-run`, live.**
Back up runs first, as the publisher does on its own.

**Stop after step 1 live** and report, as the dispatch says. Steps 2-5 then follow on my read — the
new-key tiers (2-4) have no live object, so G6 is skipped there and G6b with the named residual is
the gate; no declaration needed for them. The declaration will be needed again at **step 5
(`--timeframe annual`)** only if annual has ever been published, which it has not — first publish,
no live object, no G6.

**G5's "17 columns identical to live" passing untouched while parity failed** is worth a sentence in
the eventual CDH note: the schema is stable, the SET of hazard combinations grew, and consumers who
filter on `hazard_vars` get strictly more, never different, rows for the sets they already read.
The one exception is `PTOT-L+NTxS+PTOT-G` consumers, who must move to `PTOT-L+NTxM+PTOT-G` — that is
the #25 rename, already in the Brayden note.

---

### RESPONSE cglabs 2026-10-16 (d) — G6b PASSES with the named residual. G6 now runs for the first time and FAILs on parity, which is #13 by design. Stopped; nothing published.

Pulled `81d50b4`. Fixture, then B5 step 1 `--dry-run`. **G6b does exactly what you built it to do.**
Then G6 — which has never reached a verdict in this thread, because G6b aborted ahead of it every
time — ran, and FAILed. So step 1 did not go live. Nothing written, no relaxing flag passed.

## G6b: PASS, on all three tiers

```
[publish-tiers]   G6b named residuals loaded: TZA:coconut
1: max |ratio-1| = 0.0057 over 1359 pairs (+1 NAMED residual(s) allowed)   TRUE
  NAMED residuals, breaching but allowed with a reason (metadata/g6b_expected_residuals.csv):
[publish-tiers]   G6b ok in 0.0 min
```

The line is verbatim what you predicted. 1,360 pairs became 1,359 gated + 1 named; the bound is
untouched and the remaining population's worst is **0.0057**, a quarter of it. Same on severe,
moderate and extreme. Fixture 17/17 again. G1-G4 ok on all three; G5 ok, 17 columns identical to live.

## G6: FAIL, and only on parity

```
 1:        parity                      rows   FAIL    live 67952655 vs local 93727800
 2:        parity               hazard_vars   FAIL    live 4 vs local 5 distinct
 3:        parity                 scenarios     ok    historic,ssp126,ssp245,ssp370,ssp585
 5:     unmatched  (iso3, crop) one side only  ok    0 pairs
 6:         flips  material one side <=1% other ok    0 pairs
 7:         total  continental per crop        ok    32 crops; ratio range [0.9959, 1]; tol 5%
 8:      material  pairs within tol_pair       ok    584 pairs; 0 outside +/-25%; range [0.9763, 1.001]
 9:      material  median within tol_median    ok    median 1; tol 3%
10: small-country  pairs within tol_small      ok    161 pairs; 0 outside +/-50%
11:     livestock  control ~ 1                 ok    319 pairs; range [0.9998, 1]; tol 2%
```

**Every value population is ok, and your "G6 ≈ 1" holds exactly** — continental totals
`[0.9959, 1]`, material median 1, livestock control `[0.9998, 1]`, zero flips, zero unmatched. The
usd side has not moved. The two FAILs are both structural, and both are #13.

### The row count closes to the row

Local composition:

```
             hazard_vars        n    crops
1:      NDWS+NTx35+NDWL0 26556210       34
2:       NDWS+NTxM+NDWL0 25775145       33
3:    PTOT-L+NTxM+PTOT-G 25775145       33
4:    NDWS+THI-max+NDWL0  7810650       10
5: PTOT-L+THI-max+PTOT-G  7810650       10
                   total 93727800       44 distinct crops
```

**93,727,800 − 25,775,145 = 67,952,655 = the live row count, exactly.** The whole row-parity
difference is the single new combination `NDWS+NTxM+NDWL0`. Nothing else gained or lost a row.

### The hazard_vars count is the NTxS retirement plus that one addition

```
live  (4): NDWS+NTx35+NDWL0, NDWS+THI-max+NDWL0, PTOT-L+NTxS+PTOT-G,  PTOT-L+THI-max+PTOT-G
local (5): NDWS+NTx35+NDWL0, NDWS+THI-max+NDWL0, PTOT-L+NTxM+PTOT-G,  PTOT-L+THI-max+PTOT-G,
           NDWS+NTxM+NDWL0
```

`PTOT-L+NTxS+PTOT-G` -> `PTOT-L+NTxM+PTOT-G` is the #25 retirement of the averaged optimum, and it
carries the same 25,775,145 rows. `NDWS+NTxM+NDWL0` is the net new one. 4 -> 5.

## So I read this as the gate working, not the product failing

G6 judges the new product against the **2026-10-06 publish**, which is pre-#13 by construction: it
still carries `NTxS`. A bake whose entire purpose is to retire `NTxS` and add the per-crop optimum
*cannot* match it on hazard_vars or row count — and if it did, that would be the stop. The value
populations, which are what "has something other than #13 moved?" actually asks, are all ok.

But **`--allow-value-drift` is on the dispatch's forbidden list (line 1339), and parity is schema
shape rather than value drift anyway**, so I am not choosing a flag here, and I have not run it.

**What I need:** your call on how G6 should treat a first publish that deliberately changes the
hazard_vars set. As I see it:

1. **`--allow-value-drift` for this step**, if that flag is what carries parity past. It is the
   blunt one — it would also wave through the value populations that are currently ok, and the
   dispatch forbids it without your word.
2. **An expected-parity-move declaration**, the G6 analogue of what you just built for G6b: state
   that this publish retires `PTOT-L+NTxS+PTOT-G`, adds `NDWS+NTxM+NDWL0`, and expects rows to go
   67,952,655 -> 93,727,800, with a FAIL if the arithmetic does not close. That keeps parity a gate
   and makes it check the change we intend rather than the absence of change. It is also testable:
   the arithmetic above closes to the row.
3. **Scope parity to what #13 does not touch** — scenarios, columns, admin levels — and move the
   hazard_vars set into the declared-move population.

Option 2 reads to me like the same move that just worked for G6b, and the row arithmetic gives it a
sharp edge. Your call, not mine.

Nothing is published. Backups were not uploaded (dry run); the B4 stamp sidecars remain the only
change on disk. The moment G6 has a verdict I can act on, I run step 1 live and stop for your read
before steps 2-5.


### MACBOOK 2026-10-16 (c) — option 2, as you recommended. TZA:coconut is named with the Wete evidence. Run step 1 live.

**Your analysis settles it, and your instinct on the options is the one I took.** Option 1 widens the
bound for 1,360 pairs to excuse one. Your carve-out variant of option 2 is elegant but too forgiving:
auto-subtracting exposure from product-zero admin2 units would silently excuse a NEW regression that
wipes a district — the next Wete would never surface. Option 3 makes publish hostage to an upstream
coverage question that predates this bake and did not change in it. So: **the named list, with the
reason carried in the file** — the `cross_basis_expected_residuals.csv` discipline, applied to G6b.

**Shipped:**
- `tier_vs_exposure()` gains `allow` — a named pair that breaches is reported **ALLOWED** with its
  reason; an unnamed breach still fails; naming does not touch the bound for anything else.
- the publisher reads **`metadata/g6b_expected_residuals.csv`** and logs the rows it loads.
- the file carries one row: **TZA:coconut**, with your Wete measurement as the reason, the pre-#13
  identity (0.9762822 to the last digit), the note that it applies to every tier because the hole is
  in the hazard layer, and the instruction to remove the row when #50 resolves Wete. Decided as
  "macbook, node concurring; Pete to override".
- fixture: a named breach passes as ALLOWED; the same product fails without the naming; a list naming
  a different pair excuses nothing.

**Wete is on #50** — it is the same shape, a narrow island at 0.25°, and your Chake Chake / Mkoani
neighbours-to-the-unit table is the cleanest evidence yet that the hole is coverage, not arithmetic.

**Also noted:** your observation that gate 1 has been printing this same residual all along
(`[0.9763, ...]`) inside its deliberately loose band is the right way to see the two gates — same
ruler, different tolerances, and the tight one exists to make exactly this kind of pair say its name.

**Run step 1 after `git pull`** (expect this commit or later): dry-run, confirm the G6b table shows
`(+1 NAMED residual(s) allowed)` and the ALLOWED block names TZA:coconut with the reason — then
**live**. Stop after step 1 for my read before steps 2-5, as the dispatch says.

**Expect the same named residual on the other tiers** at steps 2-4 — the hole is in the hazard layer,
so intld/ha/prod_t will show TZA pairs against the same bound. The file already covers them; if a
tier breaches on a pair that is NOT named, that stays a stop.

---

### RESPONSE cglabs 2026-10-16 (c) — G6b: the eight border pairs are gone, one pair remains, and it is not ours. Stopped before the live run.

Pulled `0efb5b9`. Ran the publish-gate fixture, then B5 step 1 `--dry-run`. **The dry-run still FAILs
G6b, so per your instruction I stopped and did not run step 1 live.** Nothing was written; I did not
pass `--allow-exposure-mismatch`.

## The fix did what it was meant to

`max |ratio-1|` went **5.4913 -> 0.0237**, and every one of the eight admin0/admin2 border pairs has
left the table. KEN:robusta-coffee, BDI:sesameseed, SLE:banana and the rest no longer appear at any
depth. Fixture: **17/17 PASS**, including the new assertion with the admin0 row inflated 30x.

```
ALL PUBLISH-GATE FIXTURE ASSERTIONS PASSED
```

## What is left is one pair, identical on all three tiers

```
  G6b vop_usd/{severe,moderate,extreme}
    material pairs within tol   max |ratio-1| = 0.0237 over 1360 pairs   FALSE
    no product pair invents value without exposure          0 pair(s)    TRUE
    no material exposure pair missing from the product      0 pair(s)    TRUE
    product has the full scenario set   historic,...,ssp585              TRUE

      iso3           crop          E          T     ratio
 1:    TZA        coconut  120396849  117541305 0.9762822   <- the only breach
 2:    TZA        cassava 1422220300 1414068390 0.9942682
 3:    TZA   sweet-potato  549291197  546272515 0.9945044
 4:    CPV robusta-coffee       8652       8613 0.9954924
 5:    CPV        coconut    2607034    2599353 0.9970537
 6:    SEN        cassava  312375761  311572100 0.9974273
 7:    MOZ        coconut   53812532   53725253 0.9983781
 8:    MDG robusta-coffee   47141576   47081139 0.9987180
```

The same `0.0237 over 1360` on severe, moderate and extreme — so it is exposure-side, not tier-side.

**The distribution says the gate is otherwise clean.** Over all 1,360 material pairs: median
deviation **0.000003**, mean 0.000063, and

```
  dev > 0.02   :    1 pairs (0.07%)
  dev > 0.01   :    1 pairs (0.07%)
  dev > 0.005  :    3 pairs (0.22%)
  dev > 0.002  :    6 pairs (0.44%)
  dev > 0.001  :    9 pairs (0.66%)
```

One pair breaches. The second-worst is 0.0057, a quarter of the bound.

## The cause: one admin2 unit with no hazard coverage at all

Across the whole of Tanzania, exactly **one** admin2 unit carries exposure and gets zero product:

```
=== TZA admin2 units with E > 0 but product T == 0 (all crops) ===
       admin1_name admin2_name n_crops   E_lost
1: Kaskazini Pemba        Wete      13 16858272
```

Wete is on Pemba Island. It is zero in the product for **every one of the 44 crops**, while its
immediate neighbours reproduce their exposure to the unit:

```
        admin1_name admin2_name        E        T
1:     Kusini Pemba Chake Chake 81412439 81412440
3:     Kusini Pemba      Mkoani 36542084 36542083
7:  Kaskazini Pemba        Wete 16858272        0
```

Wete's coconut exposure is 2,861,351 of TZA's 120,396,849 = **2.376%**, which is the deviation to
four figures. Cassava loses the same 8,166,921 but against a 1.42 B national total, hence 0.57%.
The whole failing pair is this one unit.

The admin0 and admin2 bases now agree for TZA to 18 USD in 117.5 M (1.5e-7), so the basis change is
clean; what remains is a hole in coverage, not a mismatch of bases.

## It is not a #13 regression — the pre-#13 product gives the identical number

I read the parked pre-rebake tier,
`Data/_parked_b2_rebake/20261013_121550/hazard_risk_vop_usd/jagermeyr/haz-freq-exp_..._adm_extreme.parquet`:

```
=== PRE-#13 parked product, Wete ===
Wete non-zero crops: 0 of 44

=== PRE-#13 TZA coconut admin0 any+none by hazard_vars ===
          hazard_vars         T
1:   NDWS+NTx35+NDWL0 102866068
2: PTOT-L+NTxS+PTOT-G 117541305
picked (max) = 117,541,305  vs exposure 120,396,849  ratio = 0.9762822
```

**0.9762822, to the last digit, and 117,541,305 to the dollar.** Wete was zero in all 44 crops
before this bake as well. So this is a pre-existing coverage property that #13 did not create and
cannot have changed — note the parked file still carries `NTxS`, so it is genuinely the old product.

It is also the number gate 1 has been printing all along: gate 1's usd range was `[0.9763, 1.001]`
and it PASSED, because its band is `[0.5, 2.0]` (`usd_total_vs_reference.R:180`, deliberately loose).
G6b's `tol_pair` is 0.02. Same residual, first gate tight enough to see it.

## So I do not think this is a product-vs-input disagreement

It is a product-vs-input *agreement* everywhere the hazard layer has a pixel, and one small island
district where it has none. 0.07% of pairs, one district, 16.9 M USD of 2.4 B+ continental exposure.
Pemba is the #50 shape again — a narrow island at 0.25 degrees.

**I am not deciding this.** Your call, and the three I can see are:

1. **`tol_pair` to 0.03** on the ground that one uncovered district on Pemba is below the resolution
   the gate can speak to. Cheapest, but it widens the bound for all 1,360 pairs to accommodate one.
2. **A named exemption** — the G6b equivalent of `DRIFT_NO_REF`, listing TZA:coconut, or better, an
   `E_lost from admin2 units with zero product` carve-out computed per pair, so the gate subtracts
   provably-uncovered exposure instead of being loosened.
3. **Treat Wete as a bug to fix upstream** (the zonal extraction misses it at 0.25 deg), publish
   nothing until then. I have not checked whether 0.05 degrees covers it; I can.

Option 2 is the one that keeps the gate's teeth, if you want my read. It is the only one that stays
tight for everything else and makes the exemption say *why*.

**What I need:** which of those, or a fourth. The moment G6b passes I run step 1 live and stop for
your read before steps 2-5, as the dispatch says. Nothing is published; the stamp sidecars from B4
remain the only change on disk.


### MACBOOK 2026-10-16 (b) — G6b's exposure reader moved to admin2. Third copy of the same defect, all mine. Re-run step 1, then live.

**B4 accepted in full.** The medians landing on 1.000 rather than my predicted 0.99 — because the
0.993 was itself an artefact of the first-combination pick — is the cleanest possible confirmation
that the pick, not the product, was what the old number measured. And the spread-table deviation is
noted the right way: SYC and DJI did not go away, SOM and MUS are newly visible, and I have added
both to #50. The old gate could never have shown us Somalia and Mauritius; the spread line exists for
exactly that.

**G6b: your diagnosis is taken as given, and it is the same defect for the third time.**
`.drift_read_exposure` read the independently-zoned admin0 rows while the product's totals are formed
from the lower levels; they disagree at borders (#18). I fixed that basis in gate 1 on 2026-10-07,
fixed the aggregation pick in gate 1 again on 2026-10-16, and never carried either into the sibling
reader that G6 and G6b both use. Your twelve-row table — every worst pair exactly 1.0000 on admin2 —
is the proof, and gate 1's own output had already printed the KEN pair at 28.9 from the other
direction.

**Fixed:** `.drift_read_exposure` now sums the ADMIN2 rows per (iso3, crop). Both sides of G6 and
G6b are now on the basis the product is built from. The fixture replays your case: the synthetic
exposure table carries an admin0 row deliberately inflated 30x (the KEN robusta-coffee shape), and
the gate must land every pair on ratio 1 exactly despite it.

**Re-run step 1 after `git pull`** (expect this commit or later): dry-run first, then live.

**Expected:** G6b PASS with max |ratio-1| within the 0.02 bound — your admin2 column says the true
ratios are 1.0000 with one pair at 1.0006, so the 2 % bound has room. G6 (vs the live object) then
runs on the same basis; its ratios are drift against the 2026-10-06 publish and bounded separately.
**Stop after step 1 as the dispatch says** and report both gates' populations before steps 2-5.

If G6b still fails on any pair after the basis change, stop — that pair would be a genuine
product-vs-input disagreement, which is exactly what the gate is for.

---

### RESPONSE 2026-10-16 (B4 re-run **all PASS**, stamp PASS, **B5 step 1 ABORTS at G6b** — same defect class, other side) — cglabs — on `cf2ba04`. Gate 1 is fixed and I can see the fix working. B5 step 1 then aborted on its own exposure gate, in dry-run, with nothing written. The cause is measured below and it is **not the product**.

## B4 re-run — gate 1 PASS, gate 2 PASS, fixture PASS

| run | verdict |
|---|---|
| `usd_total_vs_reference --iso3 all` (jagermeyr) | `usd: PASS` / `intld: PASS` / **GATE PASS** |
| `usd_total_vs_reference --iso3 all --timeframe annual` | `usd: PASS` / `intld: PASS` / **GATE PASS** |
| `vop_cross_basis_gate --res 0.25` | **GATE PASS** |
| `fixture_gate_zonal_basis.R` | **ALL ASSERTIONS PASSED** |

The fixture's third assertion is the one I'd have asked for: *"admin2 basis still FAILS a product shifted 3x — the fix removes a false failure, not the gate."*

**Medians came in better than your expectation.** You said near 0.99; the actual material-pair medians are **1.000** on both sides, usd range `[0.9763, 1.001]` over 1,223 pairs, intld `[0.9759, 1.001]` over 1,198. The 0.993/0.992 medians were themselves an artefact of the first-combination pick.

**The spread line is present, so the fix reached the run:**

```
usd:   hazard_vars spread per pair — median 0.0048, max 0.9985 over 1458 pairs
intld: hazard_vars spread per pair — median 0.0054, max 0.9989 over 1399 pairs
```

**One deviation from the letter of your expectation, reported rather than smoothed over.** You expected the spread to name COM/SYC/SLE/DJI/GNB. The widest-8 table names **GNB, SOM, SLE, COM, MUS** — `GNB:robusta-coffee` 0.9985, `SOM:plantain` 0.9969, `GNB:cocoa` 0.9883, `GNB:tobacco` 0.9780, `MUS:rice` 0.9668, `SLE:banana` 0.8459, `COM:maize` 0.7875, `COM:cassava` 0.7763. **SYC and DJI are not in the top 8**, and **SOM and MUS are new names** that the old gate never surfaced. SYC and DJI haven't gone away — they simply have smaller spreads than eight other pairs, and their ratios now pass. The #50 population is therefore a little wider than the five countries we named from the failures: small islands and coastal states generally, now including Somalia and Mauritius. Worth adding to #50; not a blocker.

## Gate 3 / stamp — PASS

**18 members** on all four root x timeframe combinations (`hazard_risk_vop` and `hazard_risk_vop_usd`, each jagermeyr and annual), **9 sidecars each, 36 total**, 0 new / 36 updated. `VERDICT: PASS (expected 18 members per folder)`. Membership read from the `hazard_risk` source folder as specified.

## B5 step 1 — `--dry-run` — **G6b FAIL, tier ABORTED, nothing written**

```
Rscript scripts/r3_publish_tiers.R --variables vop_usd \
  --drift-exposure vop_usd=<...>/exposure/vop_nominal-usd-2021_adm_sum_spam20_glw420_res-25.parquet --dry-run
```

G2, G3, G4 all ok (`n(none)==n(any)` on every `hazard_vars`; five scenarios; severity `extreme`). Then:

```
  G6b vop_usd/extreme: product vs the exposure input it was built from
    material pairs within tol        max |ratio-1| = 5.4913 over 1358 pairs   FALSE
    no product pair invents value without exposure            0 pair(s)   TRUE
    no material exposure pair missing from the product        0 pair(s)   TRUE
    product has the full scenario set     historic,...,ssp585              TRUE
  G6b FAIL in 0.0 min
  ABORT vop_usd/extreme: the product does not reproduce the exposure it was multiplied by
complete in 0.4 min [DRY RUN - nothing written]
```

**I did not pass `--allow-exposure-mismatch`.** The script's own text says to use it only once the cause is understood, so here is the cause.

### It is the admin0/admin2 basis again, on the exposure side this time

G6b's product side is already correct — `r3_tier_drift_vs_live.R:77` takes `max(T)` across `hazard_vars`, the same convention you just gave gate 1. But its **exposure** side, `.drift_read_exposure`, reads

```r
dplyr::filter(is.na(admin1_name), exposure == .want)   # admin0 rows
...
ex[(tech == "all" | is.na(tech)) & is.finite(value), .(E = sum(value)), by = .(iso3, crop)]
```

— the **admin0 rows** of the 0.4.4 table. Gate 1 was moved to the **admin2** basis on 2026-10-07 precisely because those two disagree at borders (#18). G6b never got that change.

I replicated `.drift_read_exposure` exactly, including the `tech == "all"` filter, and reproduced every printed `E` to the unit. Then recomputed the same quantity on the admin2 basis:

| iso3 | crop | E (admin0, what G6b uses) | E (admin2) | T (product) | T/E admin0 | **T/E admin2** |
|---|---|---|---|---|---|---|
| KEN | robusta-coffee | 809,827 | 27,081 | 27,081 | 0.0334 | **1.0000** |
| BDI | sesameseed | 77,363 | 9,521 | 9,521 | 0.1231 | **1.0000** |
| SLE | banana | 504,419 | 3,274,343 | 3,274,343 | 6.4913 | **1.0000** |
| SLE | tobacco | 34,988 | 135,561 | 135,561 | 3.8745 | **1.0000** |
| SOM | cowpea | 13,821 | 25,612 | 25,612 | 1.8531 | **1.0000** |
| SLE | yams | 891,092 | 1,241,358 | 1,241,358 | 1.3931 | **1.0000** |
| SLE | coconut | 4,481,988 | 5,716,222 | 5,716,222 | 1.2754 | **1.0000** |
| KEN | soybean | 2,055,269 | 1,746,200 | 1,746,202 | 0.8496 | **1.0000** |
| BDI | chickpea | 43,490 | 33,842 | 33,842 | 0.7782 | **1.0000** |
| GMB | cowpea | 225,974 | 195,579 | 195,579 | 0.8655 | **1.0000** |
| SLE | robusta-coffee | 2,759,228 | 3,144,957 | 3,144,957 | 1.1398 | **1.0000** |
| SLE | cotton | 38,023 | 51,928 | 51,958 | 1.3665 | **1.0006** |

**Every one of G6b's twelve worst pairs reproduces its exposure exactly on the admin2 basis.** The identity G6b is testing holds; it is being tested against the wrong rows.

The disagreement is confined to the exposure table itself. Across 2,136 `(iso3, crop)` pairs its admin0 and admin2 totals agree at a median ratio of 1, with **8 exceptions**: `KEN:robusta-coffee` 29.90, `BDI:sesameseed` 8.13, `SLE:tobacco` 0.258, `SLE:banana` 0.154, `SOM:plantain` 0.0031, and `SLE:potato`, `SOM:pigeonpea`, `SOM:small-millet` at 0 against non-zero admin2. Gate 1 had already measured this from the other direction and printed it — *"reference by median 1.00000 (max 28.9039)"* — which is the same KEN pair.

**So: a gate defect of the same class you just fixed, in the sibling gate's exposure reader rather than its product reader.** I am not touching it, and I am not publishing past it.

## State

Nothing published. Nothing written by the dry-run. The stamp is the only thing this turn changed on disk — 36 sidecars updated, which is what B4 asked for. All parked dirs held, the nine S3 backup keys intact.

**What I need:** G6b's exposure side moved to the admin2 basis, or an explicit decision that admin0 is correct there and something else explains the eight pairs. Once G6b passes I will run step 1 live and stop for your read before steps 2-5, as the dispatch says.

Logs: `logs/r13_b4b_{fixture,g1jag,g1ann,g2,stampjob}_20261016_103921.log`, `logs/r13_b5_s1_dry_20261016_104200.log`, probe `logs/r13_b5_g6b_probe2.out`.

---


### MACBOOK 2026-10-16 — gate 1 was mine again. Fixed. Gate 2 accepted. Re-run B4, then B5.

**Your diagnosis is complete and correct, and the gate was the defect.** `.SD[1]` across `hazard_vars`
takes whichever combination sorts first — an NDWS set — and compares it against a whole-crop
reference. Where the water-balance inputs have no coverage that set is 0, so 17 correct pairs failed.
The parked pre-bake product showing identical picked totals is what settles it: nothing in the last
three days caused this.

**Fixed: the gate now takes the MAX across combinations, not the first.** That is already the
convention in the sibling gate (`r3_tier_drift_vs_live.R:77`) for exactly this reason, so this makes
the two consistent rather than inventing a rule. It also now **reports the spread across
combinations** — median and max, with the widest pairs printed when any exceeds 0.5 — because the
disagreement you found is real and must not be hidden by the max.

`fixture_gate_zonal_basis.R` gains a replay of your case: one combination zeroed for a pair while
another carries the full exposure must PASS, and the spread must be reported.

**Your product observation is filed as #50**, with your table. I have deliberately **not** decided it:
the two readings you set out have opposite implications, and choosing needs a look at the
NDWS/NDWL0 rasters on those cells rather than at R/3. It is not a #13 blocker — the exposure is
present and correct under the PTOT family, and every pair matches the reference to better than
0.03 %. **Thank you for separating "the gate is wrong" from "the product may also be telling us
something", and for not collapsing them into one verdict.**

**Gate 2 accepted, PASS.** On your three asks:

- **`TCD:yams` and `BEN:bean` — not new.** Both appear with the same ratios in the B5 thread's A4.2
  at 0.25° (`archive/dispatches/DISPATCH_cglabs_b5_fao_code_join.md`). The gate's own classification
  is right: no national constant-I$ allocation, border spill on this grid, reported not gated. My
  "any other new residual is a finding" rule is satisfied by your naming them — they are named, they
  are not new, and they stay reported rather than gated.
- **`ETH:tea`** — named residual, behaving as registered. Nothing to do.
- **Prune?** **No — keep all four CAF/GIN rows.** They are inside the band now only because B5 lifted
  the intld side 20 %; the underlying asymmetry (SPAM still carries the uncorrected tonnage) has not
  changed. Removing them would mean re-litigating the same pairs the next time intld moves. A quiet
  registered residual is cheaper than that.

**Also noted and accepted:** your correction of your own "regression" call on the 2-pair line from
the r3_res25 record. Treating it as not comparable rather than as a baseline this run broke is the
right read, and saying so explicitly is worth more than being right first time.

**Re-run B4 after `git pull`** (expect this commit or later), both gates, both timeframes.

**Expected:** gate 1 **PASS** on both `usd` and `intld`, medians near 0.99 as you measured, and a
**spread line** naming COM/SYC/SLE/DJI/GNB. If the spread line is absent, the fix did not reach the
run. If pairs are still out of band after the max, stop — that would be a different finding.

Then gate 3, the stamp, and B5.

---

### RESPONSE 2026-10-15 (B4 — gate 2 PASS, **gate 1 FAIL**; STOPPED before stamping and before B5) — cglabs — the failure is real, reproducible and **not caused by this bake**. I have not touched anything; everything after the FAIL is read-only measurement.

## Gate 1 — `usd_total_vs_reference.R --iso3 all` — FAIL

```
usd:   FAIL (median 0.993, 17 material pairs out of band, 0 invented-value pairs, n(none)==n(any) TRUE)
intld: FAIL (median 0.992, 16 material pairs out of band, 0 invented-value pairs, n(none)==n(any) TRUE)
GATE FAIL in 0.4 min
```

Your stated invariant — **material ratios near 1** — holds: medians **0.993** and **0.992**, and `n(none) == n(any)` is TRUE for all five `hazard_vars`. What fails is the per-pair `[0.5, 2]` bound, on 17 pairs concentrated in COM (8 crops at ~0.21-0.25), SLE (3), SYC (2, both exactly 0), DJI and GNB.

**It is not timeframe-scoped.** I re-ran with `--timeframe annual`, confirmed from the log header that it read the annual parquet (mtime `2026-10-14 00:28`, against jagermeyr's `2026-10-15 13:24`), and got **the identical verdict** — 17 and 16, same medians. So B2b did not do this.

### The bake did not cause it, and the product is not short

`R/checks/usd_total_vs_reference.R:117` computes the total per `hazard_vars` combination and then keeps `.SD[1]` — **whichever combination happens to sit first**:

```r
ht <- h[, .(total = sum(value, na.rm = TRUE), n_haz = .N), by = .(iso3, crop, hazard_vars)][, .SD[1], by = .(iso3, crop)]
```

I replicated the gate's own filter (`scenario == "historic"`, `hazard %in% c("any","none")`, `!is.na(admin2_name)`) and computed every combination separately, in the live product and in the parked pre-bake one. usd, annual:

| iso3 | crop | `NDWS+NTx35+NDWL0` | `NDWS+NTxM+NDWL0` | `PTOT-L+NTxM+PTOT-G` | gate picked | reference |
|---|---|---|---|---|---|---|
| SYC | coconut | **0** | **0** | 2,421,361 | 0 | 2,421,000 |
| SYC | banana | **0** | **0** | 470,066 | 0 | 470,100 |
| SLE | potato | **0** | **0** | 421,187 | 0 | 421,200 |
| GNB | robusta-coffee | 244 | 244 | 161,281 | 244 | 161,300 |
| SLE | tobacco | 32,072 | 32,072 | 135,561 | 32,072 | 135,600 |
| SLE | banana | 504,419 | 504,419 | 3,274,343 | 504,419 | 3,274,000 |
| COM | maize | 986,983 | 986,983 | 4,643,726 | 986,983 | 4,644,000 |
| COM | cassava | 2,775,487 | 2,775,487 | 12,406,550 | 2,775,487 | 12,410,000 |
| COM | banana | 3,385,925 | 3,385,925 | 14,791,861 | 3,385,925 | 14,790,000 |
| DJI | bean | 5,744,604 | 5,744,604 | 20,922,424 | 5,744,604 | 20,920,000 |

**Under `PTOT-L+NTxM+PTOT-G` all ten match the reference to better than 0.03%.** The exposure is there. The gate picks `NDWS+NTx35+NDWL0`, which is 0 or a fraction for these cells, and compares it against a whole-crop reference.

**And the parked pre-#13 product gives byte-identical picked totals** — every one of the ten, same values, same picked combination `NDWS+NTx35+NDWL0`, with `PTOT-L+NTxS+PTOT-G` (the retired set) holding the same full totals the NTxM set now holds. So this pair of behaviours predates the bake entirely. The 1,980-stack catch-up did not change which combination is picked, and did not change what it reports.

### The part that is a product observation, not a gate artefact

The two NDWS-family combinations agree with **each other** exactly in every row above, and both disagree with the PTOT family by a consistent factor. If each combination independently reproduced the full crop exposure (`any + none = 1` per pixel), they would all be equal. They are not, and the countries where they are not are COM, SYC, SLE, DJI, GNB — small island and coastal states. That is the shape of the `NDWS`/`NDWL0` water-balance inputs having no coverage on those cells while `PTOT` does. If that is expected, the gate needs to know which combination is authoritative per crop rather than taking the first. If it is not expected, the water-balance coverage on small islands is the finding, and it is older than #13.

**I am not fixing either one.** Two readings, opposite implications, and the choice is yours.

### On the "2 pairs" in the record

`archive/dispatches/DISPATCH_cglabs_r3_res25_rerun.md:399` recorded 1,226 material pairs, range `[0.4442, 1.008]`, 2 out of band, intld PASS. I called this a regression when I first saw it and that was too quick: the parked-product probe shows the same pairs behaving the same way before the bake, so the difference against that record is a reference-vintage and run-configuration difference, not something that happened in the last three days. Treat the 2-pair line as not comparable rather than as a baseline this run broke.

## Gate 2 — `vop_cross_basis_gate.R --res 0.25` — **GATE PASS**

1,396 pairs on both bases, 1,039 material. Median `nominal/intld` **1.22**, 5-95% `[0.418, 3.04]`, livestock median 1.25. Both internal checks clean: *no material pair outside `[1/10, 10]` has its nominal side off*, and *every per-crop median within `[1/5, 5]`*.

**Two out-of-band pairs that are not the pre-registered four — `TCD:yams` (ratio 184.0) and `BEN:bean` (16.4).** By your rule *any other new residual is a finding*, so I am naming them. The gate's own classification is that both have **no national constant-I$ allocation** and are border spill on this grid (#18), reported and deliberately not gated — which is why it still prints PASS. They are a finding by your rule and benign by the gate's; you own that call.

`ETH:tea` is out of band at 0.0785 and is a **named** residual in `cross_basis_expected_residuals.csv` (SPAM 2020 SSA holds 10.5 kt against FAOSTAT's 75.9 kt).

Worth a prune decision: the gate reports **all four CAF/GIN coffee rows are now inside the band or immaterial**, along with `TGO:oilpalm` and `GNB:maize`, and asks `(prune?)`. The pre-registration that made them expected is no longer doing work.

## Gate 3 — not run

I did **not** run `scripts/stamp_ensemble_membership.R`. It writes, gate 1 failed, and the dispatch says stop at a gate. The membership itself is not in doubt — the B2b log already showed **18 GCMs** by name.

## State

Nothing published, nothing stamped, nothing modified. B5 untouched. All parked dirs held: `_parked_b2_rebake/20261013_121550/`, `_parked_r22_pre13_20261015_213020/`, `hazard_risk/<tf>/_parked_NTxS_20261013_102232/`, and the nine S3 `issue9_20261008_07*` keys.

**What I need from you:** a decision on gate 1 — whether the `.SD[1]` combination pick is the defect and the gate should select per crop, or whether the NDWS-family coverage on small island and coastal states is the defect. I will not guess between them, and I will not pass a relaxing flag. Logs `logs/r13_b4_g1_20261015_220132.log`, `logs/r13_b4_g1_annual_20261015_220132.log`, `logs/r13_b4_g2_.log`, probe `logs/r13_b4_probe.out`.

---


### RESPONSE 2026-10-15 (B3 — **the #13 check PASSES**; R/2.2 re-run and CR-093 gate both clean) — cglabs — all three B3 steps done. The gate #13 exists to satisfy is met, and met with a control rather than on its own say-so.

## 1. The #13 check — PASS, both timeframes

Reference is `Data/_parked_b2_rebake/20261013_121550/` — the product parked immediately before B2, so it is the matched pre-bake object rather than something older. `poultry-highland`, **Extreme**, `hazard == "heat"` with a THI `hazard_vars`, every `iso3 x scenario x timeframe` cell:

| timeframe | dropped | rose | equal | of which `old = 0` |
|---|---|---|---|---|
| annual | **376** | **0** | 559 | 559 |
| jagermeyr | **376** | **0** | 559 | 559 |

**Nothing rose, anywhere, in either timeframe.** Every one of the 559 unchanged cells had `old = 0`, so there is no cell that could have fallen and didn't.

adm0, `ssp245` / `2041-2060`, annual — before → after, intld15-2021:

| iso3 | old | new | Δ |
|---|---|---|---|
| ZAF | 9,744,088,845 | 18,573,045 | **−99.81%** |
| KEN | 472,228,728 | 11,574,276 | **−97.55%** |
| ETH | 392,788,593 | 7,562,613 | **−98.07%** |
| MAR | 338,556,822 | 60,627 | **−99.98%** |
| UGA | 164,814,510 | 1,654,278 | **−99.00%** |
| TZA | 126,230,844 | 475,191 | **−99.62%** |
| NGA | 8,768,502 | 31,032 | **−99.65%** |
| SDN | 0 | 0 | — |

Continent totals fall between **−73.3%** and **−99.98%** across all 17 scenario x timeframe slices in both timeframes, the smallest drop being `ssp585 2081-2100` (the hottest slice, where real Extreme days survive a 89 bar) and the largest the historic baseline.

**The magnitude is enormous, so I did not take it on trust.** The control is the three livestock whose thresholds were *not* touched:

| crop | thresholds | heat&THI total, old vs new |
|---|---|---|
| `poultry-tropical` | 74 / 79 / 92 | **bit-identical** |
| `cattle-highland` | unchanged | **bit-identical** |
| `cattle-tropical` | unchanged | **bit-identical** |
| `goats-highland` | unchanged | **bit-identical** |

Only the one crop whose threshold moved, moved. That is what makes the size of the drop evidence rather than alarm: the old 79 bar was admitting essentially the entire highland-poultry VoP as Extreme, which is the defect #13 was raised for. **Extreme `poultry_highland` is now near-zero historically** — a visible product change, and one the CDH record should state rather than let be discovered.

**Two notes on the dispatch's own wording.** The thresholds are **°F** in both `metadata/haz_classes.csv` and `metadata/published_thresholds.csv` — "89 °C" in the B3 text is a slip in the prose, not in the data. And the rebaked product carries **44 crops against the parked 43**: `small-millet` is new, nothing was lost.

## 2. R/2.2 — complete, `exit=0`

738.5 s, peak RSS **27.3 GiB**. `SEC1=SEC2=SEC3=SEC4=TRUE`, all **10 parquet rewritten**, 55 iso3 in every one.

Its 18 `SEC1: WARN no future match for historic GCM` lines are **benign aliases, not losses**. Dedup leaves 36 historic entries — 18 bare GCM names plus 18 `historic_`-prefixed duplicates — and the futures are named by the bare GCM, so the prefixed half can never match. The line that settles it is `built change/diff stacks — 288 layers from 18/36 historic file(s)`, and 288 = 18 GCMs x 4 scenarios x 4 timeframes. The parked product carries the same 18 models. I would still rather these warned once with a count than 18 times by name.

## 3. CR-093 validator — **GATE PASSED, 10 PASS / 0 FAIL**

Every file iso3-bearing, WASM-prunable on non-null iso3 row-group stats, iso3-first sorted, both ensembles carrying the spread. 8.3 s.

## 4. What changed in R/2.2's output, measured

I snapshotted all 10 pre-run outputs byte-identical (10/10 md5) to `Data/_parked_r22_pre13_20261015_213020/` before launching, because R/2.2 overwrites unconditionally and has no FORCE flag.

**Every `*_by_model` file shrank; no `*_ensemble` file did.** The shrink is *exactly* the duplicate count:

| file | old | new | Δ | dups old | dups new |
|---|---|---|---|---|---|
| `ptot_change_by_model` | 2,933,568 | 2,931,264 | −2,304 | 2,304 | **0** |
| `ptot_diff_by_model` | 1,466,784 | 1,465,632 | −1,152 | — | — |
| `thi_perc_area_by_model` | 3,116,916 | 3,114,468 | −2,448 | 2,448 | **0** |
| `ntx_perc_area_by_model` | 6,233,832 | 6,228,936 | −4,896 | — | — |
| `haz_freq` | 12,467,664 | 12,457,872 | −9,792 | 9,792 | **0** |

Distinct key sets are **identical on both sides** — zero keys gained, zero lost. The old files carried duplicate rows and the new ones carry none. This is de-duplication, matching the SEC1 dedup line, not data loss.

**Values on shared keys** split three ways, and the split is informative:

| file | differing rows | where |
|---|---|---|
| `ntx_perc_area_by_model` | **0 of 6,205,680** | nowhere — wholly identical |
| `thi_perc_area_by_model` | 3,975 of 3,114,468 (0.128%) | **adm0 only** — 11.81% of adm0 rows, **0.00000% of adm1, 0.00000% of adm2** |
| `haz_freq` | 332,220 of 12,457,872 (2.67%) | all three levels — adm0 4.67%, adm1 2.68%, adm2 2.64% |

The `thi_perc` signature is the one I want your eye on. It is **exclusively adm0** — not one admin1 or admin2 row differs by any amount — and it is confined to **19 of 55 countries**, led by KEN (598 rows), ERI (584), SDN (467), EGY (459), BDI (390), with SLE and SEN also present. Median absolute difference 0.3 percentage points; 1,155 rows differ by more than 1 point, 585 by more than 10. The worst are Sudan at adm0, e.g. `GFDL-ESM4 / ssp126 / 2061-2080 / extreme` going **100.0 → 11.5**. Country-level aggregation changing while both sub-national levels stay bit-identical is the shape of a zonal-basis or border-cell change, and the country list is heavy with the coastal and border cases #18 named.

**I am reporting this, not gating on it, and here is why.** The parked R/2.2 product is dated **2026-06-24**. It predates roughly four months of pipeline fixes, so an old-vs-new diff against it spans all of them and is not a statement about #13. By this project's own rule a gate's reference has to be independent of what it judges, and this one is not — which is exactly why the #13 check in section 1 runs against the 2026-10-13 parking instead. The dispatch sets no gate on R/2.2 values, and I have not invented one. If the adm0-only `thi_perc` shift is unexpected to you, it is worth a look before publish; if it is the #18 border work landing, it is already explained.

`ntx_perc` being *wholly* unchanged is worth one line of its own: SEC3 reads the NTx mean rasters, not the interaction stacks, so the NTxS retirement and the NTxM catch-up correctly leave it alone.

## 5. Housekeeping

- The `issue9_20261008_07*` pre-publish backups are **S3 keys, not node files** — all **nine** confirmed present under `s3://digital-atlas/sandbox/backup/`. The node's local `sandbox/` is empty and always was scratch. Nothing lost; I checked rather than assumed when the empty directory turned up.
- **`hazard_risk_n` was never parked** — B2's parking covered `vop`, `vop_usd` and `ha` only — so there is no pre-#13 `head_n` anywhere on the node or in `sandbox/backup`. Not needed for the #13 check, which runs on `vop`, but worth knowing before B5 in case a `head_n` before/after is ever wanted.
- Held and untouched: `Data/_parked_b2_rebake/20261013_121550/`, `Data/_parked_r22_pre13_20261015_213020/`, `Data/hazard_risk/<tf>/_parked_NTxS_20261013_102232/`, and the nine S3 backup keys.

**Next — B4**, as the dispatch stands: `usd_total_vs_reference.R --iso3 all` on the admin2 basis with the admin0-vs-admin2 gap reported as a number, `vop_cross_basis_gate.R --res 0.25` with the four pre-registered CAF/GIN coffee rows expected and **any other new residual treated as a finding**, then `stamp_ensemble_membership.R` — 18 members, which the B2b log already showed. I will stop and report before B5 regardless of outcome.

Logs: `logs/r13_b3_r22_20261015_213020.log`, `logs/r13_b3_cr093_20261015_213020.log`.

---


### RESPONSE 2026-10-15 (B2b COMPLETE — jagermeyr §4.2 clean, 90/90, annual proven untouched) — cglabs — run finished `21:20:12 UTC`, **705.3 min** (11 h 45 m), `exit=0` from the fixed `rc=$?` capture. Every check below is measured, not inferred from the clean exit.

**The 90 files are exactly 90.** 9 parquet + 9 json per variable across all five, no more and no less:

| variable | dir | parquet | json | mtime before run start | <1 KB |
|---|---|---|---|---|---|
| `vop_intld15-2021` | `hazard_risk_vop` | 9 | 9 | 0 | 0 |
| `vop_nominal-usd-2021` | `hazard_risk_vop_usd` | 9 | 9 | 0 | 0 |
| `harv-area_ha` | `hazard_risk_ha` | 9 | 9 | 0 | 0 |
| `head_n` | `hazard_risk_n` | 9 | 9 | 0 | 0 |
| `prod_t` | `hazard_risk_prod` | 9 | 9 | 0 | 0 |
| | | **45** | **45** | **0** | **0** |

The "mtime before run start" column is the one that matters: **zero** `haz-freq-exp_*` files in any jagermeyr folder predate `09:34`, so none of the 90 is a survivor from the OOM-killed run being counted twice.

**The four checks, as run on annual.**

1. **`failed_*.txt` — absent.** No file matching `failed_*.txt` anywhere under `Data/` newer than the run start.
2. **`skipped_*.txt` — absent.** Same sweep, nothing. With `overwrite=FALSE` and an empty target, nothing was eligible to skip.
3. **Per-variable §4.2 elapsed — five non-trivial times**, none degenerate:

```
variable 1/5: vop_intld15-2021     — Complete (178.1 min)
variable 2/5: vop_nominal-usd-2021 — Complete (185.9 min)
variable 3/5: harv-area_ha         — Complete (156.7 min)
variable 4/5: head_n               — Complete ( 31.7 min)
variable 5/5: prod_t               — Complete (153.0 min)
```

`head_n` at 31.7 min is the short one, and it should be: 10 livestock categories against 44 crops, and its table is 15.6 M rows against vop's 93.7 M. The ratio of elapsed to rows is in line with the other four, so the short time is the workload, not a truncated pass.

4. **mtimes against run start — all five variables inside the window**, each cluster landing in its own variable's interval. No output carries a stale timestamp.

**Peak RSS: 137.0 GiB**, at `15:11:26`, during variable 2 (`vop_usd`). 36% of the 376 GiB box, from 1,418 samples at 30 s. For contrast the run that died peaked at **365.3 GiB** — `R3_CHECK_WORKERS=6` plus skipping §4.1 took roughly 230 GiB off the ceiling. There was never a moment in this run where OOM was in prospect.

**`dmesg` in the window — clean.** The only `Out of memory: Killed process` on this box remains `[Wed Oct 14 16:19:50 2026] ... (R) ... anon-rss:66190288kB`, which is the earlier B2 jagermeyr kill. Nothing between `09:34` and `21:23`.

**annual is untouched, measured against the pre-launch snapshot:**

| | before | after | files touched since 09:34 |
|---|---|---|---|
| `hazard_risk_vop` | 1002 / 2026-10-13 23:37:24 | 1002 / 2026-10-13 23:37:24 | 0 |
| `hazard_risk_vop_usd` | 1002 / 2026-10-14 02:42:26 | 1002 / 2026-10-14 02:42:26 | 0 |
| `hazard_risk_ha` | 823 / 2026-10-14 05:15:40 | 823 / 2026-10-14 05:15:40 | 0 |
| `hazard_risk_prod` | 823 / 2026-10-14 08:19:22 | 823 / 2026-10-14 08:19:22 | 0 |
| `hazard_risk_n` | 199 / 2026-10-14 05:47:06 | 199 / 2026-10-14 05:47:06 | 0 |

Identical counts, identical newest mtimes to the nanosecond, and a direct `-newermt` sweep returns **0** files in all five. `R3_TIMEFRAMES` did what it says.

**One extra check I ran unprompted, because these parquet go to publish.** Read `..._ENSEMBLE_int_adm_severe.parquet` for `vop` and `head_n` in both timeframes and compared the key dimensions:

| | rows | crops | hazard classes | scenarios | iso3 |
|---|---|---|---|---|---|
| annual `vop` | 93,727,800 | 44 | 9 | 5 | 55 |
| jagermeyr `vop` | 93,727,800 | 44 | 9 | 5 | 55 |
| annual `head_n` | 15,621,300 | 10 | 9 | 5 | 55 |
| jagermeyr `head_n` | 15,621,300 | 10 | 9 | 5 | 55 |

Set differences in both directions for crops and for hazards: **empty**. The two timeframes agree on structure exactly, which is what you want before five-tier publish — jagermeyr differs from annual in the values, not in the shape. (The `hazard` column at adm level carries the combination classes — `any`/`none`/`heat`/`wet`/`dry` and the four intersections — so `NTxM` vs `NTxS` is not visible at this level; that distinction lives in the `_int.tif` stacks, where it was already verified in the catch-up.)

**One loose end, minor and reported rather than resolved.** R/3 printed `There were 15 warnings (use warnings() to see them)` at exit. R defers these and prints only the count, so **the text is not recoverable from the log** and I did not re-run to chase it. Two things bound it: the setup banner's own `There were 25 warnings` appears identically in both this log and the B2 log, so that 25 is unrelated and constant; and there is **no annual baseline for the 15**, because B2 was OOM-killed before its session-end summary could print. So I cannot say whether 15 is normal for a five-variable §4.2 pass or specific to this one. Nothing in the run's measurable output is inconsistent — counts, sizes, mtimes, row counts and dimension sets are all exact — but I would rather name the gap than let the clean exit cover it. If you want them captured, `options(warn = 1)` on the next R/3 run would surface them inline at the cost of a noisier log.

**Where this leaves B2:** both timeframes now complete across §4.1 and §4.2. annual 1,203.2 min, jagermeyr §4.1 from the killed run plus §4.2's 705.3 min here. The `SKIP_R3_4_1` override stays scoped to this run and does not travel to B3.

**Next — B3**, as the dispatch stands: R/2.2 re-run (CR-093 desert mask carries automatically), `Rscript R/validate_cr093_real.R`, then the #13 check itself — `poultry_highland` Extreme exposure must **drop** against the parked product, since 89 °C is a higher bar than 79. I will report before/after at adm0 for a few countries. **If it rises or is unchanged I stop there**, because that is the bug this bake exists to fix.

Log `logs/r13_b2b_20261015_093411.log`; RSS samples `logs/r13_b2b_rss_20261015_093411.log`.

---


### RESPONSE 2026-10-15 (B2b LAUNCHED — jagermeyr §4.2 only, correctly scoped) — cglabs — `f434359` pulled, both new controls behave as described, and the run is **confined to jagermeyr with §4.1 off**. Reporting the launch now rather than at the end, since `SKIP_R3_4_1` was forbidden until this block and I want the override on the record before the result is.

**Precondition verified before relying on it, not assumed.** `SKIP_R3_4_1=1` trusts that jagermeyr's §4.1 tifs are already correct. They are: `.tif` counts match annual **exactly** across all five variables — **984 / 984 / 804 / 804 / 180** for `vop` / `vop_usd` / `ha` / `prod` / `n` — with **0 files under 1 KB** in any of them. That is the same equality that made the 90-file gap a clean §4.2-only gap.

**Launched** with `R3_TIMEFRAMES=jagermeyr SKIP_R3_4_1=1 R3_CHECK_WORKERS=6`, exactly as written. `FORCE_OVERWRITE` and `R3_ALLOW_41_FAILURES` both echoed `<unset>` from the wrapper before R started.

**Scoping confirmed in the log, not inferred:**

```
[09:34:51] entering timeframe loop over 2 periods: annual, jagermeyr
[09:34:51] R3_TIMEFRAMES active - running only: jagermeyr
[09:34:52] [jagermeyr] section 4: hazard risk x exposure (run4.1=FALSE, run4.2=TRUE)
[09:34:54] [jagermeyr] 4.2) ensemble membership: 18 GCMs
```

annual is never entered. **18 GCMs** — the membership B4 expects, visible already.

**I snapshotted annual's counts and newest mtimes before launching** (`logs/r13_b2b_annual_before_20261015_093411.txt`: 1002 / 1002 / 823 / 823 / 199 files, newest 2026-10-13 23:37:24 through 2026-10-14 08:19:22), so "nothing written under annual" will be a measured statement rather than an assurance.

**The `exit=$?` bug is fixed in this run's wrapper** — `rc=$?` on the line immediately after the Rscript, before any substitution, with the reason in the header. Every earlier log in this thread carries a meaningless `exit=0`; treat the shell's own `Killed` line as the signal when re-reading them.

**RSS sampler running** at 30 s cadence across all R processes, so the peak comes back as a number. I will also re-check `dmesg` for any OOM kill in the window rather than relying on the run looking healthy.

**On the override:** noted that `SKIP_R3_4_1` moved from forbidden to prescribed for this one case with a stated reason. I am treating it as scoped to this run, not as a general relaxation, and will not carry it into B3 or any later R/3 work unless a dispatch says so again.

**Next:** the four checks on jagermeyr's §4.2 exactly as run on annual — `failed_*` absent, `skipped_*` classes, five non-trivial per-variable elapsed times, mtimes against this run's start — plus the 9 parquet + 9 json per variable count, the peak RSS, and proof annual is untouched. Then B3. Log `logs/r13_b2b_20261015_093411.log`.

---

### MACBOOK 2026-10-15 — right to stop. Two controls added; run jagermeyr §4.2 alone.

**Your wrapper's `exit=0` catch is the most valuable thing in this report.** `echo "... exit=$?"`
with a `$(date)` ahead of it reports `date`'s status, not R's — so every run in this thread printed
`exit=0`, including a SIGKILL. You found it in your own tooling and flagged it rather than leaving
it in the record. Fix as you said: capture `rc=$?` on the line immediately after the command, before
any substitution.

**All four checks passing on what exists is the right read, and the gap is clean:** jagermeyr's
§4.1 tifs are all present, all five variables completed, and exactly the 90 §4.2 outputs are
missing. Nothing published, old product parked and intact.

**Two controls added (`R/3_freq_x_exposure.R`), because scoping this was not supported and you were
right not to improvise it:**

- **`R3_TIMEFRAMES`** — comma-separated subset of the timeframes. Stops if it names an unknown one.
- **`R3_CHECK_WORKERS`** — workers for §4.1's integrity check, default 20. That check is where the
  OOM happened, not §4.2.

**Run this:**

```
R3_TIMEFRAMES=jagermeyr SKIP_R3_4_1=1 R3_CHECK_WORKERS=6
```

`SKIP_R3_4_1=1` is the supported control for exactly this case — its header says "skip 4.1 when the
tifs are already correct", and you have verified they are. It also skips the integrity check that
did the killing, so `R3_CHECK_WORKERS` is belt-and-braces for any later run rather than load-bearing
here. **No `FORCE_OVERWRITE`, no `R3_ALLOW_41_FAILURES`.**

**On your second question:** your reading is right — the peak was at §4.1's check, not inside §4.2,
and `worker_n4.2` is already 1. annual's §4.2 completed all five variables at the same settings, so
a plain §4.2 run should pass. Watch RSS anyway and report the peak.

**Expected:** 9 parquet + 9 json per variable for jagermeyr, 90 files, nothing written under annual.
Then the four checks on jagermeyr's §4.2 as you ran them on annual, and B3.

**Growth figures noted** — `vop` +36.7 % against the ~38 % predicted for #25(a)'s extra row, and your
explanation for `vop_usd` being lower (larger post-republish baseline) is correct.

---

### RESPONSE 2026-10-14 (B2 — annual COMPLETE, **jagermeyr §4.2 killed by OOM**; all four checks pass on what exists; STOPPED) — cglabs — **annual finished both sections, all five variables, 1,203.2 min.** jagermeyr finished §4.1's five variables and was then **OOM-killed during its integrity check, so its §4.2 never ran — 90 files missing.** Your "a clean exit is not evidence of a clean run" earned itself immediately: **my own wrapper printed `exit=0` for a SIGKILLed run.** Nothing published, old product intact, nothing running.

**The kill, from the kernel, not inferred:**

```
[Wed Oct 14 16:19:50 2026] Out of memory: Killed process 3790166 (R)
  total-vm:137569112kB, anon-rss:66190288kB ... global_oom
```

One R process at **66.2 GiB RSS**; **cgroup peak 365.3 GiB of 376**. No swap, so it was instant. The log's last line is `jagermeyr 4.1) Checking file integrity`.

**My wrapper's `exit=0` is wrong and I am flagging it rather than leaving it in the record.** My script ends `echo "... DONE $(date '+%F %T') exit=$?"` — the `$(date)` substitution runs first and resets `$?`, so the field reports `date`'s status, never R's. It has printed `exit=0` on every run in this thread, including this killed one. The shell's own `3706880 Killed` line, one line above, is the only honest signal. I will capture the status into a variable before any substitution. **Had I trusted the exit line, I would have reported B2 complete.**

**The four checks, on what exists — all four pass:**
1. **`failed_risk_x_exposure_*.txt` — ABSENT everywhere**, all five variables, both timeframes.
2. **`skipped_not_in_exposure_*.txt` — exactly the three NOT_APPLICABLE classes you named**, and identical across timeframes: `harv-area_ha` 180 (the 10 livestock), `prod_t` 180 (same 10), `head_n` 594 (all 33 crops). Nothing else skipped.
3. **Per-variable §4.1 elapsed — five non-trivial entries per timeframe.** annual 91.2 / 92.8 / 143.7 / 9.9 / 146.0 min; jagermeyr 92.1 / 93.0 / 144.3 / 10.0 / 146.4 min. **Within 1 % variable-for-variable across timeframes**, the same agreement B1 and the catch-up showed. §4.2 annual 178.3 / 185.1 / 153.1 / 31.2 / 152.5 min.
4. **Output mtimes — every file in all ten directories post-dates the 12:16:42 start.** No stale carryover anywhere.

**Crop coverage complete** — no crop missing from a crop tier: `vop` and `vop_usd` carry 44 commodities (33 crops + 10 livestock + `generic-crop`), `ha` and `prod` 34 (crops + `generic-crop`, livestock correctly skipped), `head_n` the 10 livestock. Both timeframes.

**#25(a) reached the product.** `NDWS+NTxM+NDWL0` appears in R/3's output filenames alongside `NDWS+NTx35+NDWL0` and `PTOT-L+NTxM+PTOT-G`, and `NTxS` is gone. §4.1 read 7,860 input files per timeframe.

**Growth against the parked product, §4.1 `.tif` only** — `vop` 720 -> 984 (**+36.7 %**), `vop_usd` 786 -> 984 (+25.2 %), `ha` 606 -> 804 (+32.7 %). `vop` sits essentially on your predicted ~38 %; `vop_usd` is lower only because its parked baseline was already the larger post-republish one.

**Exactly what is missing: 90 files.** §4.2 writes 9 parquet + 9 json per variable. annual has 9+9 for all five; **jagermeyr has 0+0 for all five**. That is the whole of the gap — jagermeyr's §4.1 `.tif` are all present and all five variables completed.

**Old product intact and comparable**, as the parking was meant to guarantee: `Data/_parked_b2_rebake/20261013_121550/`, 4,337 files, 8.3 GB, untouched. `sandbox/backup/issue9_20261008_07*/` still held.

**Not resuming without your call, because the scoping is yours.** Re-running R/3 as launched would re-enter §4.1 for both timeframes before reaching jagermeyr's §4.2, and I will not FORCE, set `R3_ALLOW_41_FAILURES` or `SKIP_R3_4_1`, or improvise a timeframe filter. **What I need from you is the supported way to run §4.2 for jagermeyr alone** — and, since `worker_n4.2` is already 1, whether the 365.3 GiB peak means §4.2 needs something else reduced rather than simply retrying. The peak was reached at §4.1's integrity check, not inside §4.2, so a plain retry may well pass; I would rather you decide that than have me find out by running it.

**B3 is blocked behind this** — the #13 `poultry_highland` check needs both timeframes to be comparable against the parked product.

Log `logs/r13_b2_20261013_121550.log`; launch script and its deviation note in `logs/r13_b2_20261013_121550.sh` (dispatch's bare `Rscript R/3_freq_x_exposure.R` would have died at `R/3:294` on `ms_codes_url`, as B1's did — R/3 sources only `haz_functions.R` and `_helpers.R` at `:77-78`).

---

### RESPONSE 2026-10-13 (#25(a) catch-up COMPLETE — every expectation met exactly; NTxS parked; ready for B2) — cglabs — **Both timeframes, zero errors, zero dropped-combination warnings, zero guard errors.** `NDWS+NTxM+NDWL0` went **0 -> 1,980 per timeframe**, and nothing else moved. The stale `NTxS` stacks are parked and the aborted-catchup parking is deleted. Nothing is running; B2 is next.

**Gate — the four patterns, both timeframes, pre vs post:**

| pattern | pre | post | expected |
|---|---|---|---|
| `NDWS+NTxM+NDWL0` | 0 | **1,980** | ~1,980 new |
| `PTOT-L+NTxM+PTOT-G` | 1,980 | 1,980 | unchanged |
| `NDWS+NTx35+NDWL0` | 60 | 60 | unchanged |
| `PTOT-L+NTxS+PTOT-G` | 1,980 | 1,980 (now parked -> 0) | unchanged, then retired |
| total `_int.tif` | 5,220 | 7,200 | +1,980 |

**annual and jagermeyr are identical on every row.** Not ~1,980 — exactly 1,980, both.

**Timings, separate from B1's.** §5.2 annual 495.41 min on the cold pass, then 18.4 s on the warm re-run; §5.2 jagermeyr **493.01 min** — within 0.5 % of annual, the same agreement B1 showed. §5.3 annual **120.85 min**, jagermeyr **130.97 min**. Each section's own check passed: 89,760 files 0 failed at §5.2 on both timeframes, 9,840 files 0 failed at §5.3 on both. Catch-up wall-clock 19:49:38 -> 10:19:39, **14 h 30 m** including the warm §5.2 re-check.

**`NTxS` parked, not deleted.** 1,980 `*NTxS*_int.tif` per timeframe moved to `hazard_risk/<timeframe>/_parked_NTxS_20261013_102232/`. 0 `NTxS` `.tif` left in either directory; `_int.tif` back to 5,220 per timeframe, same count as before the catch-up but now composed of `NTxM` rather than `NTxS` — which is the swap #25(a) is.

**I checked the parking location before using it this time, not after.** Every live call on `haz_risk_dir` passes a pattern (`R/3:565`, `R/2:1993`, `R/3.1:107-108`), the only `recursive` mention is a `dir.create`, and `R/3.1:154,160` patterns `"parquet$"`. An `intermediate/` subdirectory **already** lives in `hazard_risk/<tf>/` alongside 240 parquet/json files, so a subdirectory there is proven tolerated rather than merely argued. Note the parking directory's own name contains `NTxS`; it is excluded from every reader by the `.tif$` patterns, but worth knowing if you grep.

**Aborted-catchup parking deleted**, as you authorised — 14 files, 12 MB, under `Data/_parked_aborted_catchup/`, gone, directory removed. I re-verified first that all 13 `.tif` names exist fresh in `hazard_timeseries_int/annual/` and are over 500 KB, so nothing was discarded that had not been rewritten.

**Still held, untouched:** `sandbox/backup/issue9_20261008_07*/`, until the bake publishes and verifies.

**B2 next.** I will park `hazard_risk_{vop,vop_usd,ha}/<timeframe>` with `mv` rather than FORCE, launch R/3 for the five variables across both timeframes with setup sourced by absolute path (R/3 reads `ms_codes_url` too, so a bare `Rscript R/3_…` would die the way B1's launch line did), then report `failed_risk_x_exposure_*.txt`, `skipped_not_in_exposure_*.txt`, per-variable §4.1 elapsed minutes and output mtimes. **Expect §4 ~38 % larger**, since `NDWS+NTxM+NDWL0` adds 1,980 stacks per timeframe to R/3's input. I will stop and report before anything is published.

---

### RESPONSE 2026-10-12 (b) (scope fix CLEAN — §5.2 annual complete; §5.3 crashed on **my** parking directory, cleared and relaunched) — cglabs — **Your scope fix is right and the gate held all the way: zero dropped-combination warnings, zero guard errors, across 8 h 43 m.** §5.2 annual wrote the full `NTxM` family — **12,240 new rasters** — and self-checked 89,773 files with 0 failures. Then §5.3 died six seconds after starting, and the cause was **mine, not yours**: the directory I parked the aborted run's files into.

**The crash:**

```
[2026-10-12 19:19:03 | +29734.3s] [2] annual | 5.2) Interactions - DONE in 29724.9s (495.41 min)
Checked 89773 file(s); 0 failed.
[2026-10-12 19:45:59 | +31350.5s] [2] annual | 5.3) Per-crop combine - START
Error in rbindlist(lapply(split_elements, as.list)) :
 Item 2 has 4 columns, inconsistent with item 1 which has 6 columns. To fill missing columns use fill=TRUE.
```

**Cause, measured.** `R/2_calculate_haz_freq.R:1769` is `list.files(haz_time_int_dir, full.names = TRUE)` with **no pattern**, so it returns directories too, and `:1771` splits each basename on `_` into exactly four columns. My `_parked_aborted_catchup_20261012_060925` splits into **six** (leading `_` yields an empty first element), and it sorts ahead of every real file, so it was item 1 and the first real file was item 2. Reproduced exactly:

```
"_parked_aborted_catchup_20261012_060925"                         -> 6
"historic_ACCESS-CM2_1995-2014_NDWS-G20+NTx20-G14+NDWL0-G5.tif"   -> 4
```

**This is my error, and it is one I should not have made — the repo already warns about it.** `R/park_stale_ensemble_stacks.R:67` says, in terms: *park OUTSIDE `hazard_timeseries_int/<tf>`: §5.3 does `list.files(haz_time_int_dir)` with no pattern and splits basenames on `_` -> a subdir there becomes a bogus model row -> stop().* I verified my parking trick against **R/3's** call, which passes `".tif$"`, and generalised from the wrong one. The existing convention parks at `Data/_parked_*/<stamp>/hazard_timeseries_int/<tf>/`.

**Cleared.** All 14 files moved to `Data/_parked_aborted_catchup/20261012_060925/hazard_timeseries_int/annual/`, following that convention — **moved, not deleted**, though they are now redundant: §5.2 rewrote all 13 names fresh, and the one the first abort truncated is now **1,014,751 bytes** against its original 4,502. Both timeframe directories now hold **0 non-`.tif` entries**. Say the word and I delete the parking.

**I audited the rest rather than just fixing the one.** Live (non-archive) unpatterned `list.files` on a hazard directory:
- `R/2:1769` `haz_time_int_dir` — the one that bit;
- `R/2:914` and `R/2:1459` `haz_time_class_dir` — **same latent trap**, nothing parked there today.

**`haz_risk_dir` is clean**: every live call patterns it (`R/3:565` `".tif$"`, `R/2:1993` `"[.]tif$"`, `R/3.1:107-108`). **So the agreed `NTxS` parking into `hazard_risk/<timeframe>/_parked_NTxS_<stamp>/` is safe** — I checked before relying on it this time rather than after.

**Relaunched 19:49:44**, same script, verified byte-identical to the approved one, `FORCE_OVERWRITE` unset and absent. §5.2 annual came back **DONE in 18.4 s** — nothing left to write, which independently confirms the 8 h 43 m pass finished its work. §5.2.1's file check runs ~27 min before §5.3 starts, so §5.3 annual is in that window now, then jagermeyr §5.2 (77,520 -> expect 89,760) and §5.3. No errors, no warnings so far.

**Timings, separate from B1's** — §5.2 annual **495.41 min** on a cold directory, **18.4 s** on a warm one. §5.3 has not yet produced a figure.

**On #46, since you offered it to my eye — the two crop-specific sets have opposite exposure, and the briefing currently caveats them together.** From the fixed flags: row 2 `PTOT-L+NTxM+PTOT-G` is F/F/F, so **both** rainfall slots take ecocrop `ropmn`/`ropmx` on the maize window — fully exposed, and it is the **replacement** for the retired `NTxS` set, so the replacement inherits #46 in full. Row 3 `NDWS+NTxM+NDWL0` — the row #25(a) adds and this catch-up is writing — is F/T/T: its wet and dry slots are **FIXED**, taken from `generic-crop`'s water balance, not from ecocrop at all. Only its heat slot is crop-derived, and that is a temperature optimum, which #46 does not touch. **So #25(a)'s row is the one crop-specific set in this bake that #46 cannot reach.** §5's table puts it under the same rainfall-window caveat as row 2; worth splitting.

**Next from me:** post-run counts for the four patterns on both timeframes, §5.3 wall-clocks, then — on a clean result — the `NTxS` parking, the parking deletes, and B2. Logs `r13_catchup3_20261012_110321` (the 8 h pass) and `r13_catchup4_20261012_194923` (live).

---

### RESPONSE 2026-10-12 (catch-up RUNNING — the gate held this time) — cglabs — `c62beee` pulled, fixture 20/20, catch-up relaunched 11:03:34 and **past the gate that stopped the previous two attempts: no dropped-combination warning, no `resolved for no crop at all` error.** §5.2 is in flight. Reporting the gate now rather than at the end, since it is the thing that failed twice.

**Fixture** — `R/checks/fixture_interaction_builder.R`, **20/20 on the node**, including the five new scope assertions and the one the whole thing turns on (`res("NTxM", FALSE, GENERIC_CROP)` is NA — the generic crop has no NTxM).

**Launch** — `logs/r13_catchup_25a_20261012_110321.sh`, verified **byte-identical** to the approved script (`diff` empty against `r13_catchup_25a_20261008_111955.sh`). `FORCE_OVERWRITE` unset in the environment and `grep -c 'FORCE_OVERWRITE=1'` = 0 in the script. Run header confirms `overwrite5.2 = FALSE` and `overwrite5.3 = FALSE`, `run5.2 = TRUE`, `run5.3 = TRUE`, sections 1/2/4 off.

**Gate — both conditions clear:**
- `could not resolve every slot` — **absent**. The six structural combinations are out of scope now rather than dropped, so the warning is empty, which is what you said it should mean.
- `resolved for no crop at all` — **absent**.
- §5.2 entered "168 combinations x 306 scen_x_model rows", same shape as before.

**Pre-run counts, both timeframes identical** (recorded in `logs/r13_catchup3_pre_20261012_110321.txt`) — `NDWS+NTxM+NDWL0` **0** | `PTOT-L+NTxM+PTOT-G` 1,980 | `NDWS+NTx35+NDWL0` 60 | `PTOT-L+NTxS+PTOT-G` 1,980 | 5,220 `_int.tif` total. The `_parked_aborted_catchup_20261012_060925/` directory is untouched, so all 14 of those names are absent and get written fresh.

**Next from me, unprompted:** the post-run counts per timeframe against those four patterns, the §5.2 and §5.3 wall-clocks quoted separately from B1's, then — on a clean result — park the stale `NTxS` stacks, delete the aborted-catchup parking as you authorised, and start B2. If §5.3 writes 0 new `NDWS+NTxM+NDWL0`, or the warning appears after all, I stop there instead.

**On the instruction:** noted, and it cuts both ways — I would not have had the `generic-crop` / `NTxM` answer to hand you if the over-tight gate had not made me go and measure it.

---

### MACBOOK 2026-10-12 (b) — you were right to stop, and the fault was my INSTRUCTION. Scope tightened. Resume.

**The 6 drops are structural, exactly as you diagnosed, and my "stop if the warning appears at all"
was an instruction that a correct run could never satisfy.** That is the failure mode AGENTS.md puts
first — a gate stated so tightly that a good run fails it — and I wrote it into the dispatch three
days after insisting on the opposite. Stopping on a stated expectation was the right call; the
expectation was the thing that was wrong.

**Your diagnosis is complete and I have taken it as given.** `NTxM` is the per-crop ecocrop optimum;
`generic-crop` is the absence of a crop and carries only `NTx35`/`NTx40`; so a free heat slot can
never resolve there. My scope filter was one-sided — it kept **every** row under `generic-crop`,
including the two that cannot resolve there — so the warning could never be empty.

**Fixed as an either/or, which is what scope actually is:**

```r
X <- X[(heat_fixed & wet_fixed & dry_fixed) == (crop_focus == GENERIC_CROP)]
```

All-fixed rows only under the generic crop; any row with a free slot only for real crops. Row 1 still
emits its 60; rows 2 and 3 still emit per crop; the six structural combinations are now out of scope
rather than dropped. **The warning becomes meaningful again** — it should be EMPTY, and a row in it
now means a real crop is missing a threshold it needs, which is the case worth seeing (a commodity
with no ecocrop match, say). I have said so in the warning text.

The fixture now exercises the scope rule instead of grepping for it, including that the generic crop
has no `NTxM` — the fact the whole thing turns on. 20 assertions.

**Resume: re-run the same script unchanged**, after `git pull` (expect this commit or later).

**Expected now:**
- **no** `resolved for no crop at all` error, and **no dropped-combination warning at all**;
- §5.2 writes the absent `NDWS-G*+NTx*-G*+NDWL0-G*` names, leaving the 43,860 existing ones alone;
- §5.3 writes **~1,980 new `NDWS+NTxM+NDWL0_int.tif` per timeframe**, with `PTOT-L+NTxM+PTOT-G` still
  1,980 and `NDWS+NTx35+NDWL0` still 60.

**If the dropped-combination warning appears this time, stop and report it** — with scope fixed it
should be empty, so a row in it is a real finding rather than noise. That is now a gate that can hold.

**Your handling of the abort was right.** Parking the 14 touched files, including the 4,502-byte
part-written raster, rather than deleting them, and verifying the restart sees the exact pre-run
directory, is what made "re-run the same script unchanged" safe to say. Keep
`_parked_aborted_catchup_20261012_060925/` until the catch-up succeeds; delete it then, or at the
same time as the `NTxS` parking — it has no further value once the 14 names are written fresh.

Then park the stale `NTxS` stacks as agreed, then B2.

---

### RESPONSE 2026-10-12 (catch-up re-run — fix WORKS, but STOPPED on the dropped-combination warning as instructed) — cglabs — `c5a282f` resolves the FIXED slots: §5.2 started writing the previously-absent `NDWS-G*+NTx{20,30,33,36}-G*+NDWL0-G*` names within a minute. **But the dropped-combination warning still appears (6 combinations), and you said stop if it appears at all — so I stopped, 2 min in, before §5.3.** The 6 are **structural, not a defect**: all are `crop = generic-crop`, rows 2 and 3, × 3 severities, and **only the heat slot is NA**.

**Fixture first.** `R/checks/fixture_interaction_builder.R` passes all 15 assertions on the node, including the two new ones (`GENERIC_CROP` is the post-rename spelling; nothing is addressable as `generic` after the rename).

**Launched** `logs/r13_catchup_25a_20261012_060925.sh`, byte-identical to the script you approved, `FORCE_OVERWRITE` verified unset in the environment and absent from the script (`grep -c 'FORCE_OVERWRITE=1'` = 0). Run header confirmed `overwrite5.2 = FALSE` and `overwrite5.3 = FALSE`.

**The fix works.** No `resolved for no crop at all` error. §5.2 entered "168 combinations x 306 scen_x_model rows" and in ~1 min wrote **13 new `_int.tif`** under names that did not exist before the run — `NDWS-G15+NTx20-G7+NDWL0-G2`, `…NTx30-G21…`, `…NTx33-G14…`, `…NTx36-G7…` across ACCESS-CM2 / ACCESS-ESM1-5 / CanESM5. Those are exactly the `NTxM` names #25(a) was missing.

**What I stopped on, measured rather than inferred.** I replayed R/2's head (read-only, lines 1-535, stops before §1) and printed `.c_dropped` with its `crop` column, which the warning's own print omits:

```
         crop severity_class heat_simple dry_simple wet_simple heat_na dry_na wet_na
 generic-crop       Moderate        NTxM     PTOT_L     PTOT_G    TRUE  FALSE  FALSE
 generic-crop       Moderate        NTxM       NDWS      NDWL0    TRUE  FALSE  FALSE
 generic-crop         Severe        NTxM     PTOT_L     PTOT_G    TRUE  FALSE  FALSE
 generic-crop         Severe        NTxM       NDWS      NDWL0    TRUE  FALSE  FALSE
 generic-crop        Extreme        NTxM     PTOT_L     PTOT_G    TRUE  FALSE  FALSE
 generic-crop        Extreme        NTxM       NDWS      NDWL0    TRUE  FALSE  FALSE
```

`haz_class[crop == "generic-crop" & index_name2 == "NTxM"]` is **empty** (0 rows). The generic crop carries `NTx35` and `NTx40` and no `NTxM` — `NTxM` is the per-crop ecocrop optimum threshold, so by construction it cannot exist for a crop that is the absence of a crop.

**So the warning is reachable on every future run, not a one-off.** The scope filter at `:511` is `X <- X[!(heat_fixed & wet_fixed & dry_fixed) | crop_focus == GENERIC_CROP]` — the right-hand clause keeps **every** row for `generic-crop`, including the two whose heat slot is free and per-crop. They then necessarily fail to resolve. The all-fixed row 1 is unaffected and resolves for `generic-crop` as intended.

**Nothing is lost by the drop.** Rows 2 and 3 are per-crop rows and are still emitted for all the real crops; a `generic-crop` copy of them would be meaningless, since the free slot has no generic value to take. Pre-run on-disk counts agree that this is not a regression: `NDWS+NTx35+NDWL0` = 60 per timeframe (row 1, generic-crop, 60 stacks per crop) and `PTOT-L+NTxM+PTOT-G` = 1,980 per timeframe (per-crop, 33 crops × 60), i.e. rows 2 and 3 produced nothing for `generic-crop` in the previous bake either.

**Not fixed here.** The one-line shape that would silence it is to let `generic-crop` through only for all-fixed rows — but that is your call, and the guard is yours. Your expectation "no dropped-combination warning" is the thing that cannot currently hold; the product expectation can.

**State of the node — clean, pre-run, nothing half-written.** The kill caught one raster mid-write (`historic_CanESM5_…NDWS-G25+NTx30-G21+NDWL0-G8.tif`, 4,502 bytes against a normal 0.86-1.07 MB, plus its `.ovr.tmp`). I **parked, did not delete**, all 14 files the aborted run touched into `hazard_timeseries_int/annual/_parked_aborted_catchup_20261012_060925/`. A non-recursive `list.files` cannot see them, so the restart sees exactly the pre-run directory: 0 non-`NTx35` `NDWS+NTx*+NDWL0` names, the 1,020 pre-existing `NTx35` ones untouched, 77,520 `.tif` in the directory. B1's products were never in scope and are untouched.

**Pre-run counts, both timeframes identical** — `NDWS+NTxM+NDWL0` 0 | `PTOT-L+NTxM+PTOT-G` 1,980 | `NDWS+NTx35+NDWL0` 60 | `PTOT-L+NTxS+PTOT-G` 1,980 (the stale ones awaiting parking) | 5,220 `_int.tif` total.

**Wall-clock**: catch-up ran 06:09:25 → 06:12 (stopped by me). §5.3 was never reached, so there is no §5.3 figure to quote separately yet.

**Ready to go the moment you clear it.** Say the word — either "the 6 generic-crop drops are expected, resume" or send a tightened filter — and I re-run the same script unchanged, confirm ~1,980 new `NDWS+NTxM+NDWL0` per timeframe, then park the `NTxS` stacks and start B2. If you tighten the filter I will re-run the fixture first. The parked `_parked_aborted_catchup_20261012_060925/` stays until you say otherwise; on resume all 14 names are absent and will be written fresh.

---

### MACBOOK 2026-10-12 — my fix had a one-token bug; the guard caught it. Fixed. Re-run the catch-up.

**The guard earned its place on its first outing, against the person who wrote it.** Nothing was
written, the B1 products are untouched, and you had the cause measured — 0 rows under `generic`, 42
under `generic-crop` — rather than inferred. That is the whole point of stopping at a gate.

**Cause confirmed and fixed.** `R/2:325` renames `generic` → `generic_crop` → `generic-crop` before
the builder runs, and `.resolve_slot()` asked for the pre-rename spelling, so every FIXED slot
returned NA. That is also why **row 1 failed as well as row 3** — it is all-fixed, so all three of
its slots went through that path. Row 2 survived because its three slots are free and per-crop.

`GENERIC_CROP` is now derived from the same expression as the rename, asserted non-empty at §0.2.2.4,
and used in both places the builder needed it. The literal is gone.

**The fixture was blind to this for the same reason the last one was.** It built its synthetic
`haz_class` with `crop = "generic"` and so never modelled the rename it was supposed to be testing
through. It now uses `GENERIC_CROP`, lifted from R/2, and asserts that nothing is addressable as
`generic` after the rename. Twice now I have written a fixture that tested my intent instead of the
code's path; that is the lesson from this one, not the token.

**Re-run the catch-up, same form as before** (`git pull` first, expect this commit or later):

```
SKIP_R2_RUN1=1 SKIP_R2_RUN2=1 SKIP_R2_RUN4=1 RUN_R2_RUN5_2=1 RUN_R2_RUN5_3=1   # no FORCE_OVERWRITE
```

**Expected now:**
- §0.2.4.2 prints **no** "resolved for no crop at all" error and **no** dropped-combination warning;
- §5.2 writes the absent `NDWS-G*+NTx*-G*+NDWL0-G*` names and leaves the 43,860 existing ones alone;
- §5.3 writes **~1,980 new `NDWS+NTxM+NDWL0_int.tif` per timeframe**, with `PTOT-L+NTxM+PTOT-G` still
  1,980 and `NDWS+NTx35+NDWL0` still 60.

**If it writes 0 again, or if the dropped-combination warning appears at all, stop and report.**

Then park the stale `NTxS` stacks as agreed, then B2.

**B1 itself is accepted** — 40 h, zero errors, both timeframes matching at 43,860 / 3,240, peak RSS
266.8 GiB with 109 GiB of headroom. Quote the catch-up's wall-clock separately from §5.3's.

---

### RESPONSE 2026-10-10 (B1 COMPLETE + catch-up STOPPED by the new guard) — cglabs — R/2 finished both timeframes cleanly at 03:12 (40 h 00, zero errors, peak RSS 266.8 GiB). Catch-up then **stopped at the new guard before writing anything**: 2 of 3 crop rows resolve for no crop. Cause is one token — `.resolve_slot()` looks up `crop == "generic"`, but `R/2:324` has already renamed that crop to **`generic-crop`**, so every FIXED slot returns NA. Not fixed here.

**B1 is complete.** `logs/r2_rebake_20261008_111955.log`, 2026-10-08 11:22 → 2026-10-10 03:12.
`Script 2 - timeframe loop completed.` Zero lines matching `error|halted`. Every integrity check passed.

| section | annual | jagermeyr |
|---|---:|---:|
| 1 Classify | 193.0 min | 192.4 min |
| 2 Frequency | 64.3 min | 62.3 min |
| 2.1 Freq ensemble | 11.4 min | 11.7 min |
| 3 Crop risk stacks | 36.8 min | 37.8 min |
| 4 Mean/SD | 39.4 min | 39.3 min |
| 4.1 Mean ensemble | 2.9 min | 2.8 min |
| 5.2 Interactions | 514.5 min | 506.8 min |
| 5.3 Per-crop combine | 273.6 min | 279.0 min |

Invariant 4 — **peak RSS 266.8 GiB** (2026-10-09 12:15, jagermeyr §3, 20 workers), 109 GiB of headroom
against 376 GiB; no swap was touched. §5.2 and §5.3 sit at 15-37 GiB. Each timeframe wrote 43,860
interaction rasters (§5.2) and 3,240 `_int.tif` stacks (§5.3), the two timeframes matching exactly.

**The catch-up stopped at your new guard, which is the guard doing its job.** Run
`logs/r13_catchup_25a_20261008_111955.log`, launched with `FORCE_OVERWRITE` verified unset in-log and
`SKIP_R2_RUN1=1 SKIP_R2_RUN2=1 SKIP_R2_RUN4=1 RUN_R2_RUN5_2=1 RUN_R2_RUN5_3=1`. It never reached §5.2:

```
[2] 0.2.4.2) WARNING: 105 (crop x severity x interaction) combination(s) could not resolve every slot and are dropped.
   heat_simple dry_simple wet_simple
1:        NTxM     PTOT_L     PTOT_G
2:        NTxM       NDWS      NDWL0
Error: 0.2.4.2) 2 of 3 crop interaction rows resolved for no crop at all: NTx35+NDWS+NDWL0, NTxM+NDWS+NDWL0.
```

Nothing was written: pre-run `NDWS+NTxM+NDWL0` count was 0 on both timeframes and remains 0, and the
run aborted before any raster call. The products from B1 are untouched.

**Cause, confirmed on the node.** `.resolve_slot()` resolves a FIXED slot with
`src <- fifelse(fixed, "generic", crop_focus)` and then `haz_class[crop == src[k] & ...]`. But
`R/2:324` runs first:

```r
haz_class[crop == "generic", crop := "generic_crop"][, crop := gsub(" |_", "-", crop)]
```

so by the time the builder runs there is no crop called `generic` — it is `generic-crop`. Measured on
the real `metadata/haz_classes.csv` after that line: rows with `crop == "generic"` = **0**, rows with
`crop == "generic-crop"` = **42**. So every FIXED slot resolves to NA. That is why exactly the two
rows carrying the FIXED `NDWS`/`NDWL0` pair fail while row 2, whose three slots are all free and
resolve per crop, survives. The 105 dropped combinations are 35 crops × 3 severities for row 3.

The same literal appears in the scope filter one line below,
`X <- X[!(heat_fixed & wet_fixed & dry_fixed) | crop_focus == "generic"]`, where `crop_focus` is drawn
from `haz_class[, unique(crop)]` and is therefore `generic-crop` too. So the all-fixed row 1 is now
emitted for **no** crop at all, where before it was emitted under the generic crop — which is why row 1
joins row 3 in the stop, and why B1's existing 60 `NDWS+NTx35+NDWL0` stacks are named
`generic-crop_<model>_<sev>_NDWS+NTx35+NDWL0_int.tif`.

**Why the new fixture passed anyway:** `fixture_interaction_builder.R` builds its own realistic
`haz_class`. If that fixture's generic rows are labelled `generic` rather than `generic-crop`, it
exercises the builder against a crop vocabulary the real pipeline does not produce. Worth pinning the
post-`:324` spelling in the fixture, so the fixture and the pipeline agree on what the crop is called.

**Not fixed here**, per the dispatch. It looks like a one-token change plus the matching literal in the
scope filter, but the builder is yours and a wrong guess here costs another pass. **Nothing is running
on the node now.** The moment this lands I re-run the same catch-up script unchanged, confirm ~1,980 new
stacks per timeframe (0 means it still did not reach the run), then park the retired `NTxS` stacks and
start B2.

---

### MACBOOK 2026-10-09 — both findings accepted. Builder FIXED. Let R/2 finish, then catch up §5.2/§5.3, then park NTxS.

**Finding 1 is correct in every particular, and it is my bug.** `metadata/haz_classes.csv` carries
NDWS and NDWL0 only under `crop = generic`; `R/2:142` replicates the generic non-heat rows per
species for **livestock** and nothing does the same for crops, which get the ecocrop-derived rows
instead. So the mixed row mapped `dry` and `wet` to NA for every real crop and the `!is.na` filter
dropped it. Row 2 survived on per-crop PTOT, row 1 survived only under `generic` — exactly your 60.

**Your sharpest line is the one about the fixture.** `fixture_crop_heat_interactions.R` pinned the
table and the published labels, and both were right. Neither says the row resolves to anything. A
declaration is not an output, and I asserted the declaration.

**Fixed on `develop`** (`.resolve_slot`): a `*_fixed = TRUE` slot now resolves from the `generic`
rows — which is what "fixed" means — and a free slot from the crop's own. Scope follows the flags
rather than being assumed: a row with any free slot is emitted per crop, an all-fixed row once under
`generic`. That reproduces rows 1 and 2 and both animal rows exactly, and makes row 3 resolvable.
I deliberately did **not** emit the all-fixed row per crop: it would write ~34 copies of one
identical hazard stack and cost R/3 the same multiple for nothing.

Two guards added, because this failed silently for a whole pass: R/2 now **stops** if a declared
interaction row resolves for no crop at all, and reports any partially-resolved combinations it
drops. New `R/checks/fixture_interaction_builder.R` runs the real builder against a realistic
`haz_class` and asserts each declared row resolves for the crops it should — eight fixtures now.

**Finding 2 accepted, and your proposed pre-step is right.** Park them:
`mv` the 1,980 `*PTOT-L+NTxS+PTOT-G*_int.tif` in each timeframe into
`hazard_risk/<timeframe>/_parked_NTxS_<stamp>/`. A subdirectory is invisible to R/3's non-recursive
`list.files`, so that is sufficient and reversible. **Park, do not delete.** Do the same for any
other retired name you find when the run ends.

**Sequence from here — do not restart the running R/2.**

1. **Let the current R/2 finish** (jagermeyr pass). Nothing it is producing is wrong; it is missing
   one family. Restarting would throw away ~20 hours to no purpose.
2. `git pull` once it is done.
3. **Catch-up run, exactly as you proposed:** §5.2 then §5.3 with `overwrite = FALSE` so only the
   absent `NDWS+NTxM+NDWL0` names are written. Nothing to pre-delete. Use the §5.3 run controls;
   do **not** set `FORCE_OVERWRITE`, which would redo everything.
   **Expected: ~1,980 new `_int.tif` per timeframe** (the same count as the `PTOT-L+NTxM+PTOT-G`
   family, since both are per-crop × 3 severities), and `NDWS+NTx35+NDWL0` still 60.
   **If the catch-up writes 0, stop** — that means the fix did not reach the run.
4. **Then park the NTxS stacks**, then B2.

**On invariant 3:** my +38 % was the estimate of a row that produced nothing, so +2.0 % was the
honest measurement of what actually ran. After the catch-up, §5.3's total for the bake will land
near the +38 % shape. Quote the catch-up's own wall-clock separately rather than folding it in.

**Noted for the record:** your B1-launch deviation was right too — R/2 does not source setup, and
the AGENTS.md §2 form is correct. The dispatch's bare `Rscript R/2_calculate_haz_freq.R` is my
error; I have left B1 as you corrected it.

---

### RESPONSE 2026-10-09 (B1 annual complete) — cglabs — §5.3 annual 273.6 min vs 268.3 last bake (**+2.0 %, not +38 %**) — the number behind the interim finding. Annual timeframe done 06:50; jagermeyr pass under way. R/2 still running.

Annual, `logs/r2_rebake_20261008_111955.log`: §1 193.0 · §2 64.3 · §2.1 11.4 · §3 36.8 · §4 39.4 · §4.1 2.9 ·
§5.2 514.5 · **§5.3 273.6 min** (2026-09-14: 268.3). `hazard_risk/annual`: 5,880 fresh files, of which 3,240
`_int.tif` stacks — `NDWS+NTx35+NDWL0` 60, `NDWS+THI-max+NDWL0` 600, `PTOT-L+NTxM+PTOT-G` 1980, `PTOT-L+THI-max+PTOT-G` 600; **`NDWS+NTxM+NDWL0` 0**.
Peak RSS over the annual pass 260.8 GiB (§3); 211 GiB at the §5.3 integrity check / jagermeyr §1 hand-over.
The 1,980 stale `NTxS` stacks per timeframe are untouched, as reported. Nothing else deviates.

---

### RESPONSE 2026-10-09 (B1 interim — FINDING) — cglabs — the #25(a) third crop row `NDWS+NTxM+NDWL0` is in the table but PRODUCES NOTHING: §5.2 wrote 129 combinations and none pairs a crop-specific NTx threshold with NDWS/NDWL0; §5.3 is writing `PTOT-L+NTxM+PTOT-G` stacks (1,023 so far) and zero `NDWS+NTxM+NDWL0`. Cause located (haz_classes.csv has NDWS/NDWL0 only for `generic`; the combination builder ignores `*_fixed`). Invariant 3 (+38 %) will therefore NOT hold. R/2 left running — the omission does not corrupt the rest. Second item for B2: 1,980 stale `NTxS` stacks per timeframe would be ingested by R/3 §4.1 unless parked.

**Where the run is** (`logs/r2_rebake_20261008_111955.log`, annual timeframe): §1 193.0 min, §2 64.3 min, §2.1 11.4 min,
§3 36.8 min, §4 39.4 min, §4.1 2.9 min, **§5.2 514.5 min** (full recompute under FORCE; the 2026-09-14 figure of
1.8 min was a skip-existing run, so there is no like-for-like baseline), §5.3 started 02:16 and is writing to
`Data/hazard_risk/annual/` (~55 files/min, 15 workers). Peak RSS so far 260.8 GiB (16:20, §3, 20 workers);
§5.2/§5.3 sit at 18-37 GiB. jagermeyr follows.

**Finding 1 — the new interaction row is dropped before it reaches §5.2.**

Evidence, from the files:
- §5.2 fresh output (`hazard_timeseries_int/annual`, 43,860 files, 129 distinct combinations): the only
  `NDWS+NTx*+NDWL0` names are the three generic severities `NDWS-G15+NTx35-G7+NDWL0-G2`,
  `NDWS-G20+NTx35-G14+NDWL0-G5`, `NDWS-G25+NTx35-G21+NDWL0-G8` (1,020 files = 1 combo family × 3 sev × 340
  scen-model). Crop-specific heat thresholds (`NTx20…NTx38`, 13 of them, × G7/G14/G21) appear ONLY paired with
  `PTOT-L…+PTOT-G…`. No `NDWS-G15+NTx30-G7+NDWL0-G2`-shaped file exists.
- §5.3 fresh output after 72 min: `PTOT-L+NTxM+PTOT-G_int.tif` 1,023; `NDWS+NTx35+NDWL0_int.tif` 60 (generic
  crop only); `NDWS+NTxM+NDWL0_int.tif` **0**. The k-loop writes every combo of a (crop, sev, model) together,
  so 1,023 of one and 0 of the other is structural, not ordering.
- The log's table does carry the row (`3: NTxM NDWL0 NDWS FALSE TRUE TRUE crop`), so the checkout is current.

Cause (`R/2_calculate_haz_freq.R:468-485`): `combinations_c` is built per (crop, severity) by mapping each
row's `heat_simple / dry_simple / wet_simple` through `haz_class[crop == crop_focus & description ==
severity_focus]` with `replace_exact_matches()`, then `combinations_c[!is.na(heat) & !is.na(dry) & !is.na(wet)]`.
The `*_fixed` flags are never consulted there. `metadata/haz_classes.csv` carries `NDWS` and `NDWL0` **only
under `crop = generic`** (3 rows each; 0 rows for any named crop), so for every real crop row 3 maps `dry` and
`wet` to NA and is dropped. Row 2 survives because `PTOT_G/PTOT_L` are per-crop in the CSV and `NTxM`
thresholds are generated per crop from ecocrop at run time (`:228-333`). Row 1 survives only for the generic
crop, which is why `NDWS+NTx35+NDWL0` has exactly 60 stacks. The fixture `fixture_crop_heat_interactions.R`
pins the table and the labels, not the builder, so it passed.

**Not changed.** No code edited, R/2 not stopped. What a fix needs, for the macbook: the builder has to
resolve a `*_fixed = TRUE` hazard from the `generic` rows (that is what "fixed" means), or the CSV needs
per-crop NDWS/NDWL0 rows. Once fixed, the missing combos can be produced without a full re-run: §5.2 with
`overwrite = FALSE` writes only the absent names (the runbook's pre-delete + overwrite=FALSE pattern, nothing
to delete here), then §5.3 the same way — §5.3's own cost for the missing ~1,000 stacks per timeframe is
small next to today's. That can run after this R/2 completes and before B2.

**Invariant 3 consequence:** §5.3 will come in near the 2026-09-14 figure (268 / 249 min), not +38 %, because
the third row contributes nothing. That is the deviation; it is explained.

**Finding 2 — stale retired-family stacks will be read by R/3 §4.1 unless parked.** `hazard_risk/annual` and
`hazard_risk/jagermeyr` each hold **1,980 `*_PTOT-L+NTxS+PTOT-G_int.tif`** from the last bake. FORCE never
touches them (different name), and R/3 §4.1 takes `list.files(haz_risk_dir, ".tif$")` through
`.rebake_scope`, which is identity by design (`R/3:565`, `.rebake_scope <- function(files) files`). So B2 as
written would ingest the retired `NTxS` family alongside `NTxM` and the tiers would carry both labels —
contradicting the "RETIRED, not reused" assertion the fixture makes about the published set. Proposed B2
pre-step, for the macbook to confirm: `mv` the 1,980 `*NTxS*_int.tif` in each timeframe dir into
`hazard_risk/<timeframe>/_parked_NTxS_<stamp>/` (a subdirectory is not listed by the non-recursive
`list.files`), alongside the dispatch's parking of `hazard_risk_{vop,vop_usd,ha}`. Same for any other
retired name found when the run ends (none seen so far).

Also for the record: `hazard_timeseries_int/annual` holds 33,911 pre-run files beside the 43,860 fresh ones;
§5.3 reads by exact combo name from `haz_int_file_tab`, so stale extras there are inert.

---

### RESPONSE 2026-10-08 (B1 launch) — cglabs — R/2 running since 11:22 with `FORCE_OVERWRITE=1 RUN_R2_RUN3=1 RUN_R2_RUN5_3=1`. One deviation: the dispatch's bare `Rscript R/2_calculate_haz_freq.R` dies at once (`object 'ms_codes_url' not found`) — R/2 does not source setup itself; relaunched in the AGENTS.md §2 form. Invariants 1-2 already hold: three crop rows (NTx35 / NTxM / NTxM); ecocrop "every SPAM commodity matched" (no no-match list, so no `tomatoes` either). 3-4 follow at completion.

**The deviation.** First launch, exactly as B1 is written: log ends at line 2 with
`Error: object 'ms_codes_url' not found`. `ms_codes_url` is defined in `R/0_server_setup.R:621`;
`R/2_calculate_haz_freq.R` lists setup as a prerequisite in its header (`:37`) and never sources it
(`:107` sources only `haz_functions.R`). The 2026-09-14 bake's log opens with the setup banner, so that
run was launched with setup sourced. Relaunched from the repo root as

```
FORCE_OVERWRITE=1 RUN_R2_RUN3=1 RUN_R2_RUN5_3=1 nohup Rscript -e 'source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); source("/home/jovyan/atlas/hazards_prototype/R/2_calculate_haz_freq.R")' > logs/r2_rebake_20261008_111955.log 2>&1 &
```

(setup's `Sys.setenv(project_dir = getwd())` at `:111` is what R/2's `Sys.getenv("project_dir")` needs,
so the cwd at launch matters: repo root). The aborted log is kept as
`logs/r2_rebake_20261008_111955_ABORTED_nosetup.log`. **B2's R/3 line has the same shape** — R/3 also
reads `ms_codes_url` (`R/3:294`) — and will be launched the same way unless told otherwise.

**Invariant 1 — crop_interactions: three rows, heat NTx35 / NTxM / NTxM.** Checkout is current.

```
   heat_simple wet_simple dry_simple heat_fixed wet_fixed dry_fixed   type
1:       NTx35      NDWL0       NDWS       TRUE      TRUE      TRUE   crop
2:        NTxM     PTOT_G     PTOT_L      FALSE     FALSE     FALSE   crop
3:        NTxM      NDWL0       NDWS      FALSE      TRUE      TRUE   crop
   (animal: THI_max+NDWL0+NDWS, THI_max+PTOT_G+PTOT_L — two rows, unchanged)
```

**Invariant 2 — ecocrop:** `every SPAM commodity matched an ecocrop species`. The dispatch expected
`tomatoes` in a no-match table; there is no table. Reporting as seen, not reconciled — if `tomatoes`
was expected to be unmatched, something matched it (the 44-crop list printed above it starts
`arabica coffee / Coffea arabica, banana / Musa acuminata, …`).

Invariants 3 (§5.3 count + wall-clock vs 268 / 249 min; expect ~+38 %) and 4 (peak RSS, from
`logs/r13_rss_20261008_111955.log`, 60 s samples of all R processes) are reported when R/2 finishes
— hours, both timeframes. `timeframes = annual jagermeyr` confirmed in the log.

---

### RESPONSE 2026-10-08 (B0) — cglabs — on `09a396c`: 7/7 fixtures PASS; `probe_r2_5_2_vec.R` **PROBE PASS** on node terra (haz_sum and ensemble mean/stdev identical), so `USE_R2_5_2_VEC` stays ON; THI_max poultry_highland Extreme = 89 present; reference + both family twins carry the B5 mtimes at both resolutions; 123 T free of 192 T. B5 clean-up done. B1 launched 11:2x.

```
PASS vop_allocate | PASS fao_code_join | PASS r3_physical_tiers | PASS publish_tier_gates
PASS gate_zonal_basis | PASS crop_heat_interactions | PASS qaqc_denominator
haz_sum PASS — functionally identical (values + missingness + any_haz)
ensemble PASS — terra::mean/stdev identical to per-layer loop
PROBE PASS — all §5.2 vectorizations functionally identical
```

Logs: `logs/r13_b0_fixtures_20261008_111955.log` (+ one `.out` per fixture),
`logs/r13_b0_probe_r2_5_2_vec_20261008_111955.log`.

Pre-conditions: `metadata/haz_classes.csv:73` = `THI_max,Extreme,3,>,poultry_highland,89`.
`Data/exposure/{exposure_adm_sum_spam20-20_glw420-20,vop_intld15-2021_adm_sum_…,vop_nominal-usd-2021_adm_sum_…}_res-{05,25}.parquet`
mtimes 2026-10-07 19:57-20:02 (the B5 A3 run). `df`: 123 T free of 192 T (37 % used).

**B5 clean-up (authorised in the archived thread's close block):** deleted on the node, plain `rm`:
`fao_prices/crop_factor_intld15-2021-t_res-25.tif` (26.7 MB), `sandbox/stage0_harness_20261007_184538/`
(557 MB; symlink targets `nex-gddp-cmip6/` and `chirps_wrld/` confirmed intact afterwards),
`sandbox/b5_old_adm_sum_20261007_193653/` (7 MB). **Nothing on S3 touched**; the
`s3://digital-atlas/sandbox/backup/issue9_20261008_07*/` backups stay.

**B1 launched:** `FORCE_OVERWRITE=1 RUN_R2_RUN3=1 RUN_R2_RUN5_3=1 Rscript R/2_calculate_haz_freq.R`,
`logs/r2_rebake_20261008_111955.log`; an RSS sampler (`logs/r13_rss_20261008_111955.log`, every 60 s,
all R processes) runs beside it for the peak-memory line. Baseline for the +38 % invariant, from the
last bake (`logs/r2_ens_5_3_20260914_074411.log`): §5.2 132 combinations × 306 scen_x_model; §5.3
annual 268.3 min, jagermeyr 248.5 min (44 crops × 3 sev × 20 models). Expect ~370 / ~343 min.

---

> **RELEASED 2026-10-08. The precondition is met: B5 completed A0-A6, published nine keys and verified
> every one MD5 == ETag off S3** (`archive/dispatches/DISPATCH_cglabs_b5_fao_code_join.md`). This
> thread is now live — start at B0.

**What B5 changed under this bake, and what it means for the gates here:**

- **The constant-I$ reference is now 240.24 B I$, up from 200.28 (+20.0 %)** — the recovered composite
  value. Every intld product this bake builds inherits that. It is correct, and it is large; the
  CDH records and the note to Brayden must state it.
- **The nominal side is unchanged** (201.92 B, and the family key published row-identical). So the
  `vop_usd` tier, which is the one step here with a live object to drift against, should show
  **G6 ≈ 1**. A large move on the usd tier would mean something other than B5 moved, and is a stop.
- The physical twins the new tiers gate against are confirmed present in the res-25 table:
  `prod`/`t`, `harv-area`/`ha`, `number`/`number`.
- The crop QAQC denominator was fixed mid-thread (it medianed a composite group's items instead of
  summing them). It now reads **0.98 / 0.99** with livestock at 242/242. If it reads near 1.17 again,
  the checkout is stale.

Thread opened 2026-10-07 (macbook). Runbook: `R/NEXT_FULL_REBAKE.md`. Context and decisions:
`HANDOVER_2026-10-07.md` §2. Roughly a working day per stage — read the whole dispatch before
launching anything.

**There is no Stage-0 refresh before this bake** (settled 2026-10-07): water-balance v2 is blocked on
inputs (#45) and the #14 HSH fix runs afterwards with its thresholds. R/2 reads today's indices.

---

## What is different from the last bake

Five changes land in this run. Each is pinned by an off-node fixture; run them all at B0.

| change | effect on the run |
|---|---|
| **#41 physical tiers** — `prod_t` and `head_n` now in R/3's `to_do_list` | R/3 §4 does **5** variables, not 3 |
| **#25(a)** a third crop interaction row, `NDWS+NTxM+NDWL0` | **§5.3 and R/3 §4 are about 38 % larger than last time.** Expected, not a runaway |
| **#25(b)** crop heat family `NTxS` → `NTxM` | published `hazard_vars` renamed; no extra compute |
| **B5** FAO code join + CAF/GIN pins | already landed in the exposure reference by the B5 thread |
| **publisher** `--variables` / `--model` axes, and the new **G6b** gate | the publish step is five commands, not one |

---

## Gates — stop at every one

### B0. Fixtures and pre-conditions, before anything long

```
cd /home/jovyan/atlas/hazards_prototype
git fetch && git log --oneline -1 origin/develop && git pull
for f in vop_allocate fao_code_join r3_physical_tiers publish_tier_gates gate_zonal_basis crop_heat_interactions qaqc_denominator; do
  echo "--- $f"; project_dir=$PWD Rscript R/checks/fixture_$f.R >/dev/null 2>&1 && echo PASS || echo FAIL
done
```

All seven must PASS. Then, and this one is **not optional**:

```
Rscript R/probe_r2_5_2_vec.R
```

`USE_R2_5_2_VEC` defaults **ON** and its terra identity probe has only ever run on macbook terra
(runbook item 3). **If it fails, export `USE_R2_5_2_VEC=0` for the whole bake and say so in the
RESPONSE.** A silent vectorisation difference in §5.2 would propagate into every tier.

Confirm before launching:
- `metadata/haz_classes.csv` has `THI_max,Extreme,3,>,poultry_highland,89` (the #13 fix itself);
- the exposure reference and both family twins on disk carry the B5 mtimes, at **both** resolutions;
- disk headroom — the last report was 123 T free of 192 T, and this run adds two new tier trees.

### B1. R/2 — both timeframes

```
export FORCE_OVERWRITE=1 RUN_R2_RUN3=1 RUN_R2_RUN5_3=1
# R/2 does NOT source setup itself (`ms_codes_url` is defined in 0_server_setup.R), so the bare
# `Rscript R/2_calculate_haz_freq.R` dies at line 2. AGENTS.md §2 form, absolute paths:
nohup Rscript -e 'source("/home/jovyan/atlas/hazards_prototype/R/0_server_setup.R"); source("/home/jovyan/atlas/hazards_prototype/R/2_calculate_haz_freq.R")' \
  > logs/r2_rebake_$(date +%Y%m%d_%H%M%S).log 2>&1 &
```

**`§3` and `§5.3` are toggle-only — `FORCE_OVERWRITE` alone does NOT enable them** (`run3` at
`R/2:658`, `run5.3` at `R/2:705`). Without both toggles the crop-stack and per-crop interaction
producers are skipped, `haz_risk/` never refreshes, and R/3 §4.1 silently consumes stale stacks.
`run5.2` already follows `FORCE_OVERWRITE`, so `RUN_R2_RUN5_2` is not needed separately.

**Report from the log, as invariants:**
1. The printed `crop_interactions` table — **three** crop rows, heat `NTx35`, `NTxM`, `NTxM`. If it
   shows two rows, the checkout is stale: stop.
2. The `0.2.2.1)` ecocrop line — either "every SPAM commodity matched" or the no-match table. We
   expect `tomatoes` to appear; **anything else in that list is a finding**.
3. §5.3 combination count and wall-clock against the last bake. **Expect roughly +38 %**, because
   crops went from two interaction rows to three. Materially more than that is worth stopping on.
4. Peak RSS. No swap, 376 GiB, OOM is an instant kill.

### B2. R/3 — five variables, both timeframes

**Park, do not FORCE.** `mv` the existing `hazard_risk_{vop,vop_usd,ha}/<timeframe>` aside rather
than overwriting, so a failed run leaves the old product intact and comparable. `hazard_risk_prod`
and `hazard_risk_n` are new and start empty.

```
nohup Rscript R/3_freq_x_exposure.R > logs/r3_rebake_$(date +%Y%m%d_%H%M%S).log 2>&1 &
```

Defaults are correct: `R3_CROP_VOP_USD=2021`, `run4.1` on, `run4.2` on. **Do not set
`R3_ALLOW_41_FAILURES`** — a silent §4.1 failure is what hid #9 for months.

**A clean exit is not evidence of a clean run.** Before trusting it, check all four:
- `failed_risk_x_exposure_*.txt` — absent or empty, for every one of the five variables;
- `skipped_not_in_exposure_*.txt` — expect livestock × `prod_t`, livestock × `harv-area_ha` and
  crops × `head_n` as classified NOT_APPLICABLE skips. **That is correct behaviour, not a failure.**
  Any crop missing from a crop tier is a finding;
- per-variable §4.1 elapsed minutes — five non-trivial entries;
- output mtimes against the run start, per variable and timeframe.

### B3. R/2.2 and the validators

R/2.2 re-runs and the CR-093 desert mask carries automatically. Then:

- `Rscript R/validate_cr093_real.R` (note: it checks structure, **not value ranges** — see
  `archive/dispatches/ISSUE_cr093_nan_zeroprecip.md`, so a PASS is not a statement about the numbers)
- **the #13 check itself:** `poultry_highland` **Extreme** exposure must **DROP** against the parked
  product — 89 °C is a higher bar than 79. Report the before/after at adm0 for a few countries.
  **If it rises or is unchanged, stop: that is the bug this bake exists to fix.**

### B4. Gates, before any publish

```
Rscript R/checks/usd_total_vs_reference.R --iso3 all          # --basis admin2 is now the default
Rscript R/checks/vop_cross_basis_gate.R --res 0.25
```

- `usd_total_vs_reference` now gates on the **admin2 basis** and *measures* the admin0-vs-admin2 gap
  separately (#18's border cells, as a number). **Invariant: material ratios near 1.** The printed
  zonal-basis line is informational.
- `vop_cross_basis_gate`: the four CAF/GIN coffee rows are **pre-registered** in
  `metadata/cross_basis_expected_residuals.csv` — expected, because B5 corrected the FAO tables and
  deliberately not SPAM. **Any other new residual is a finding.**

Then stamp ensemble membership from the `hazard_risk` source folder:
`Rscript scripts/stamp_ensemble_membership.R`. **18 members, or the publisher refuses.**

### B5. Publish — five variables, in the no-regrets order

One command per step, each independently gated, **each with its own `--drift-exposure` basis**. Run
every one with `--dry-run` first and report the gate output before the live write. Background the
live ones and log to `logs/` — a publish was SIGTERM'd mid-run on 2026-09-28.

Bases (all `_res-25`, the grid the bake ran on), under `<exposure_dir>`:

| tier | `--drift-exposure` basis |
|---|---|
| `vop_usd` | `vop_nominal-usd-2021_adm_sum_spam20_glw420_res-25.parquet` |
| `vop_intld` | `vop_intld15-2021_adm_sum_spam20_glw420_res-25.parquet` |
| `ha`, `prod_t`, `head_n` | `exposure_adm_sum_spam20-20_glw420-20_res-25.parquet` |

```
# 1. the proven path first, against a live object
Rscript scripts/r3_publish_tiers.R --variables vop_usd   --drift-exposure vop_usd=<usd twin>
# 2-4. new keys: G6 cannot run (no live object), so G6b is the gate
Rscript scripts/r3_publish_tiers.R --variables vop_intld --drift-exposure vop_intld=<intld twin>
Rscript scripts/r3_publish_tiers.R --variables ha        --drift-exposure ha=<reference>
Rscript scripts/r3_publish_tiers.R --variables prod_t    --drift-exposure prod_t=<reference>
Rscript scripts/r3_publish_tiers.R --variables head_n    --drift-exposure head_n=<reference>
# 5. then the same five with --timeframe annual
```

**Stop after step 1 and report** before doing 2-5. It is the only step with a live object to drift
against, so it is the one that proves the bake end-to-end against something known.

**`--model ENSEMBLE` and per-GCM are step 6 and are NOT in this dispatch.** They widen the notebook
read contract and need Brayden's confirmation first.

Do not pass `--allow-schema-drift`, `--allow-value-drift`, `--allow-exposure-mismatch`,
`--no-exposure-gate` or `--skip-gates` unless a later block here says so.

### B6. Post-publish

- the CR-068 probes: `probe_no_hazard_arithmetic_quick.sh <ISO3>` and
  `probe_cross_parquet_vop_drift.sh <ISO3>` against canonical S3. **Both have known bugs that read as
  data defects** — see the runbook's post-bake section before interpreting either.
- the self-contained check instead: `any + none` from the product itself, crossed to
  `crop-livestock_all_res-25.parquet` (the unsuffixed key is the 0.05° alias; #9/#12 closed on
  exactly that mistake).
- diff local against S3 per key. The uploader always overwrites and never skips, so a stale key needs
  an explicit delete — and **`s3fs` deletes are PERMANENT**, so back up first and use `paws`
  `delete_object()` without a VersionId if one is genuinely needed.

---

## What NOT to do

- **Do not start before the B5 thread finishes.** The reference must be the republished one.
- **Do not set `R3_ALLOW_41_FAILURES`, `SKIP_R3_4_1` or `--skip-gates`.**
- **Do not FORCE over the existing R/3 outputs** — park them.
- **Never `s3fs::s3_file_delete()` / `s3_dir_delete()`.** Permanent, no delete marker.
- **Do not publish the model axis or `value_sd`** — out of scope here.
- **Do not improvise a fix at any gate.** Stop and describe what you see.

## Open questions this bake forces, for the macbook side — not node work

- **#37:** MapSPAM 2020 is CC-BY-SA-4.0 and share-alike propagates into `hazard_exposure`. Four new
  CDH records get authored off this bake, so the licence question has to be answered then.
- **#20:** publish the generated threshold table from `metadata/haz_classes.csv` with the release.
- **Consumer note:** the crop-specific `hazard_vars` is renamed `PTOT-L+NTxM+PTOT-G` and
  `NDWS+NTxM+NDWL0` is new; `PTOT-L+NTxS+PTOT-G` is retired. The live V1 notebook reads
  `NDWS+NTx35+NDWL0`, unchanged. Brayden needs this with the key list on `data-management#2`.

## Reporting

Prepend a `### RESPONSE <date>` block per block completed — do not wait until the end; this runs for
days. Then verify it landed on `origin/develop`: `git fetch`; `git log origin/develop..HEAD` empty;
`git show origin/develop:DISPATCH_cglabs_issue13_rebake.md | grep -c RESPONSE`.

Commit trailer: `Co-Authored-By: <your own model name> <noreply@anthropic.com>`.
