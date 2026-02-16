
# K8DNZ / The Cadence Project (Rust)

**Last updated:** 2026-02-16  
**Status:** Experimental, but we now have a proven “time-indexed reconstruction” pipeline — plus a deterministic banding + tags primitive (`orbexp bandsplit`) that can generate low-alphabet “timelines” (TG1 tags). We also falsified the current conditioning-as-XOR-mask approach (it destroys fit), so the next step is new conditioning semantics where tags select mapping/field variants rather than scrambling bytes.

## Current progress (2026-02-16)

We have a new, concrete “vision-aligned” path that is showing real signal:

- ✅ `timemap gen-law` now generates TM1 + residual from a deterministic **law** (no window-scanning required).
- ✅ A new bitfield mapping family, **low-pass threshold** (`--bit-mapping lowpass-thresh`), produces **structured bits** (runs/streaks) so the residual becomes more zstd-friendly.
- ✅ We reached **57.31% matches** on a 1024-byte target (bitfield(1), rgbpair, lowpass-thresh, closed-form law).
- ✅ On a 512-byte target, we got **effective_no_recipe = 303 vs plain_zstd = 272** (**only +31 bytes** overhead), meaning we are **closing in on plain zstd** while still using a deterministic “program + patch” architecture.

### Current formula (math string)

We are currently using **law-driven indexing + low-pass threshold bit extraction + XOR residual**.

**Law-driven index selection (TM1)**
\[
\mathrm{TM1}[i] = s + i \quad\text{for } i=0..N-1
\]
where \(s\) (the chosen start) is produced deterministically by the selected law (`jump-walk` or `closed-form`) over the legal window \(W\).

**Low-pass threshold bit extraction**
\[
\hat{x}[t] = \mathbf{1}\{\,\mathrm{LP}(I(t);\tau,\text{smooth\_shift}) > \theta\,\}
\]
(`lowpass-thresh` smooths a deterministic intensity signal from the generator and thresholds it into 0/1.)

**Residual (xor)**
\[
r[i] = x[i] \oplus \hat{x}[\mathrm{TM1}[i]]
\]

> Important: match% is not the only metric; we care about **residual compressibility**. Low-pass helps even when match% is moderate, because it makes the residual less noise-like.

### New way to run (current best path)

Example (512-byte target, bitfield(1), lowpass-thresh, jump-walk law):

```bash
cargo run -p k8dnz-cli -- timemap gen-law \
  --recipe ./configs/tuned_validated.k8r \
  --target /tmp/g1_512.bin \
  --out-timemap /tmp/g1_512_lp.tm1 \
  --out-residual /tmp/g1_512_lp.bf \
  --map bitfield \
  --mode rgbpair \
  --bits-per-emission 1 \
  --bit-mapping lowpass-thresh \
  --bit-tau 128 \
  --bit-smooth-shift 3 \
  --bitfield-residual packed \
  --chunk-size 128 \
  --law-type jump-walk \
  --start-emission 0 \
  --search-emissions 2000000 \
  --max-ticks 20000000 \
  --residual xor \
  --zstd-level 3
```

Recent scoreboard highlight (512 bytes):

* plain_zstd_bytes = 272
* tm1_zstd_bytes = 25
* residual_packed_zstd_bytes = 278
* effective_no_recipe = 303  (**+31 bytes vs plain zstd**)

Recent scoreboard highlight (1024 bytes, closed-form, tau=160, smooth_shift=7):

* matches = 4695/8192 (57.31%)
* plain_zstd_bytes = 445
* tm1_zstd_bytes = 33
* residual_packed_zstd_bytes = 445
* effective_no_recipe = 478 (**+33 bytes vs plain zstd**)

---

K8DNZ is a deterministic codec prototype inspired by a simple but powerful mechanism:

* Two “dots” (A and C) free-orbit on equal-circumference circles in opposite directions at different speeds.
* When their phases align within a window ε, they enter **lockstep** on a truncated cone (frustum) whose small rim is B.
* In lockstep, the pair moves in perfect tandem on opposite sides (Δ = 0.5 turns by default), spiraling to the large rim.
* At the rim, we sample a deterministic **field**, emit a deterministic token, and the next cycle begins.

The core output is a time-sensitive token stream: if the “time” (tick index) is off, emissions change—or don’t appear. That “time determinism” is the backbone of the project’s long-term **ARK key** vision.

---

## Status (what works today)

### ✅ Deterministic round-trip is proven (ARK container)

* `encode` → produces a `.ark` artifact that embeds the recipe and ciphertext
* `decode` → reproduces the original bytes exactly
* tests confirm:

  * **Genesis1.txt roundtrip matches bytes**
  * **two identical runs produce byte-identical `.ark`**

### ✅ Timemap + Residual reconstruction is proven (the “impossible → possible” milestone)

We now have a second, more important proof:

> Using only a recipe + a timing map (TM1) + a residual patch, we can reconstruct target bytes exactly via deterministic regeneration.

This is implemented as:

* `timemap fit-xor` / `timemap fit-xor-chunked` → finds windows in the generated stream and produces:

  * a **TM1** (time positions / indices into the stream)
  * a **residual** (patch bytes; historically XOR, now also supports other residual semantics and bitfield residual payloads)
* `timemap reconstruct` → regenerates the stream at those indices and applies the residual to produce the exact target

This has been validated with `cmp ... OK` across:

* small slices (first line / 57 bytes)
* 256 bytes
* full `text/Genesis1.txt` (~4201 bytes)
* both `pair` and `rgbpair` modes

This is the current “core win”: deterministic time indexing + small side-data makes perfect reconstruction possible.

### ✅ RGB stream extraction + indexing semantics are correct

We support two byte-stream views:

* **Pair stream**: `tok.pack_byte()` → 1 byte per emission
* **RGBPair stream**: `tok.to_rgb_pair().to_bytes()` → 6 bytes per emission (flattened)

Important indexing rule (now enforced):

* `pair` mode positions: `pos == emission_index`
* `rgbpair` mode positions: `pos == emission_index*6 + lane` (flattened byte stream)

This allows TM1 to reference *exact byte positions* even in rgbpair mode.

### ✅ Optional mapping/permutation layer exists and is reversible

We can optionally apply a mapping layer (e.g., `SplitMix64`) during fitting and reconstruction.

This adds a degree of freedom:

* it can reshuffle or transform stream bytes deterministically
* we still reconstruct exactly as long as reconstruction uses the same mapping parameters

This remains a lever for residual reduction.

### ✅ Instrumentation exists for analysis and diagnostics

* `analyze` prints byte histograms + entropy + zstd ratio
* `ark-inspect --dump-ciphertext` extracts ciphertext for analysis
* `encode --dump-keystream` dumps the keystream used for XOR
* timemap runs print a **scoreboard** (recipe bytes, TM1 bytes/zstd, residual bytes/zstd, effective totals)

---

## New milestone: Bitfield timemap mapping (bitfield → lanes/timelines)

We now support a **bitfield** mapping view for timemap fitting and reconstruction. Instead of treating the target purely as a byte stream, we can treat it as a stream of small symbols and track **which time positions emitted which symbol**.

This is invoked via:

* `--map bitfield`
* `--bits-per-emission {8,2,1}`
* `--bit-mapping {geom,hash,lowpass-thresh}` (variants)
* optional `--time-split` (timeline-style residual packing)

And validated end-to-end:

* `timemap fit-xor-chunked (bitfield)` produces TM1 + a bitfield residual payload
* `timemap reconstruct (bitfield)` reproduces the original bytes exactly **as long as `--max-ticks` is at least what the fit used**

  * we initially hit `reconstruct short` when reconstruct used a lower default tick cap
  * adding `--max-ticks ...` to reconstruct fixed it (`reconstruct ok`)

### What we learned (important)

* **Bitfield(8)** (256 symbol lanes) reconstructs correctly, but residual is still large.
* **Bitfield(2)** (4 symbol lanes → `00/01/10/11`) reconstructs correctly and is currently the best-performing bitfield run of this session.

  * It also increased match rates significantly (~25–33% matches per 128–512 symbol chunk in many segments), indicating we’re capturing more structure.
* **Bitfield(1)** is structurally interesting: match rates jump dramatically (often ~55–60% matches per 512-symbol chunk), suggesting the generator is “closer” to a 1-bit view.

  * But it increases symbol count and requires more ticks / stream length to chain across the whole target.
* Smaller bits-per-emission increases symbol count and generally requires **more ticks**; tick budget matters (we saw “no room to finish” at low tick limits and fixed it by increasing `--max-ticks`).

### The new “timeline” idea is now concretely realized

Bitfield(2) lanes directly correspond to 4 timelines:

* `00` timeline
* `01` timeline
* `10` timeline
* `11` timeline

This aligns strongly with the frustum “color band” vision: low-alphabet control over where the data lives.

### Lane / timeline analysis tooling

We added:

* `timemap bf-lanes` → inspects a `.bf2` residual payload and reports:

  * lane counts / distribution (e.g., for 2-bit: 4 lanes `00/01/10/11`)
  * per-lane bitset sizes and zstd sizes
  * totals vs baseline packed payload zstd

This is primarily an **analysis tool**: naïvely splitting lanes into separate compressed bitsets usually loses cross-lane redundancy, but the lane distribution skew is extremely informative for “timeline” strategies.

---

## New milestone: `orbexp bandsplit` produces deterministic lanes + TG1 tags (banding + timelines)

We added orbital experiments that treat fixed-size blocks as inputs to a deterministic “meet-time” scan, then turn that into a practical primitive:

* `orbexp blockscan` computes meet-time statistics per block (diagnostic / measurement).
* `orbexp bandsplit` deterministically assigns each block to a lane/bucket and can emit:

  * an adjacency-preserving block stream (`.data.bin`)
  * a tag stream (`.tags.bin`) in either byte-per-tag or packed **TG1** format

This yields a tiny, deterministic **routing / band ID** per block that matches the “color walls / bands on the frustum” vision: an orderly low-alphabet control signal that can define lanes/timelines (e.g., 2-bit tags → `00/01/10/11`).

### Key discovery: MOD choice can be degenerate vs informative (very important)

With Genesis1, 128-bit blocks, `bucket_fn=tfirst`, `bucket_mod=4`:

* Under `mod = 2^32 (4294967296)`, lane assignment can become degenerate:

  * for shifts 0..16 we observed lanes_used = 1 (all blocks map to lane 0)
  * even at shift=29 we saw lanes_used = 3 (lane 3 unused)

* Under `mod = 2^32 - 1 (4294967295)`, the same setup produces all 4 lanes with meaningful distribution, e.g.:

  * lane0: 33 blocks
  * lane1: 27 blocks
  * lane2: 68 blocks
  * lane3: 134 blocks

Interpretation:

* This is not invertible decoding (collisions are expected and fine).
* But it is a powerful deterministic primitive.
* `mod = 2^32` can collapse lane entropy; `mod = 2^32 - 1` restores entropy and is now the default for banding experiments.

---

## What we’re building (end product)

The end product is a **deterministic expansion key system**:

> A compact ARK key (recipe + seeds + mapping rules + length + checksum) deterministically regenerates large outputs—pages of text, bytes, or structured tokens—without needing the original input file.

This has two related goals:

1. **Deterministic expansion** (already real): a short key can generate large reproducible streams.
2. **Compression-by-model** (actively underway): for structured data (e.g., text), fit a cadence recipe + mapping + timing map so that the target can be reconstructed with a **small, compressible residual**.

We are currently validating the system end-to-end using **Genesis1.txt** as the canonical sample.
We scale to larger text only after the pipeline is proven on Genesis.

---

## Core concepts

### Fixed-point “turns” (no floats)

All phase/time evolution uses integer fixed-point turns. No π in core logic. (π/τ is optional for visualization only.)

### Deterministic field sampling

At emission time, we sample a deterministic field model at known coordinates derived from:

* lockstep phase (phi_l)
* paired phase (phi_l + Δ)
* axial parameter (t at the rim)
* engine time (ticks)

Then we clamp + quantize deterministically to emit tokens.

### Quant shift as a distribution knob

A key design knob is `quant.shift`—it moves bin boundaries without altering cadence timing.

Important update: we observed that certain “tuned” recipes can become degenerate (regen produces all-zero tokens/bytes), while validated configs produce healthy entropy. This is now a tracked issue: **recipe tuning must never produce a degenerate stream**.

### Two output layers

* **PairToken layer**: compact and stable for token pipelines; packable to 1 byte per emission.
* **RGBPair layer**: 6 bytes per emission; can be palette-mapped or field-driven, and is now fully supported by TM1 via flattened indexing.

### The “Double Helix” viewpoint

A successful reconstruction can be thought of as two interlocking strands:

* **Strand 1 (Time / Index / Curve):** the TM1 timing map – which positions matter
* **Strand 2 (Value / Patch):** the residual – what must be applied at those positions

Together, they reconstruct the target perfectly from a deterministic generator.
The major remaining goal is to make these strands **small** (especially the residual).

---

## Repo layout (high level)

* `crates/k8dnz-core/` — deterministic cadence engine, field model, recipe, token types
* `crates/k8dnz-cli/`  — simulator, encoder/decoder, timemap tools, inspect/analyze tools, experiments (`orbexp`)
* `text/Genesis1.txt`  — canonical sample input used for experiments/tests

---

## Quickstart

### Build

```bash
cargo build
```

### Simulate (pair tokens)

```bash
cargo run -p k8dnz-cli -- sim --emissions 10 --mode pair --fmt jsonl
```

### Simulate (rgbpair)

```bash
cargo run -p k8dnz-cli -- sim --emissions 10 --mode rgbpair --fmt jsonl
```

### Encode Genesis1.txt → .ark

```bash
cargo run -p k8dnz-cli -- encode --in text/Genesis1.txt --out /tmp/genesis1.ark --profile tuned --max-ticks 50000000
```

### Decode .ark → original bytes

```bash
cargo run -p k8dnz-cli -- decode --in /tmp/genesis1.ark --out /tmp/genesis1.decoded.txt --max-ticks 50000000
diff -u text/Genesis1.txt /tmp/genesis1.decoded.txt | head
```

### Inspect .ark + dump ciphertext

```bash
cargo run -p k8dnz-cli -- ark-inspect --in /tmp/genesis1.ark --dump-ciphertext /tmp/genesis1.cipher.bin
```

### Analyze bytes (entropy + histogram)

```bash
cargo run -p k8dnz-cli -- analyze --in /tmp/genesis1.cipher.bin --top 16
cargo run -p k8dnz-cli -- analyze --in text/Genesis1.txt --top 16
```

### Dump & analyze the raw keystream used for XOR

```bash
cargo run -p k8dnz-cli -- encode --in text/Genesis1.txt --out /tmp/genesis1.ark --profile tuned --max-ticks 50000000 --dump-keystream /tmp/genesis1.keystream.bin
cargo run -p k8dnz-cli -- analyze --in /tmp/genesis1.keystream.bin --top 16
```

---

## Timemap pipeline (the current reconstruction MVP)

### 1) Build a target slice (example: first 256 bytes of Genesis1)

```bash
head -c 256 text/Genesis1.txt > /tmp/gen256.bin
```

### 2) Fit a window + residual against the deterministic stream (pair mode)

```bash
cargo run -p k8dnz-cli -- timemap fit-xor --recipe ./configs/tuned_validated.k8r --target /tmp/gen256.bin --out-timemap /tmp/gen256.tm1 --out-residual /tmp/gen256.resid --search-emissions 2000000 --max-ticks 80000000 --start-emission 0
```

### 3) Reconstruct exact bytes using recipe + TM1 + residual

```bash
cargo run -p k8dnz-cli -- timemap reconstruct --recipe ./configs/tuned_validated.k8r --timemap /tmp/gen256.tm1 --residual /tmp/gen256.resid --out /tmp/gen256.out --max-ticks 80000000
cmp /tmp/gen256.bin /tmp/gen256.out && echo OK
```

### RGBPair mode (flattened 6-byte-per-emission stream)

RGBPair fitting uses flattened indices: `pos = emission*6 + lane`.

```bash
cargo run -p k8dnz-cli -- timemap fit-xor --recipe ./configs/tuned_validated.k8r --target /tmp/gen256.bin --out-timemap /tmp/gen256_rgb.tm1 --out-residual /tmp/gen256_rgb.resid --mode rgbpair --search-emissions 2000000 --max-ticks 80000000 --start-emission 0
cargo run -p k8dnz-cli -- timemap reconstruct --recipe ./configs/tuned_validated.k8r --timemap /tmp/gen256_rgb.tm1 --residual /tmp/gen256_rgb.resid --out /tmp/gen256_rgb.out --mode rgbpair --max-ticks 80000000
cmp /tmp/gen256.bin /tmp/gen256_rgb.out && echo OK
```

### Optional mapping layer (example: SplitMix64)

```bash
cargo run -p k8dnz-cli -- timemap fit-xor --recipe ./configs/tuned_validated.k8r --target /tmp/gen256.bin --out-timemap /tmp/gen256_rgb_mapped.tm1 --out-residual /tmp/gen256_rgb_mapped.resid --mode rgbpair --map splitmix64 --map-seed 1 --search-emissions 2000000 --max-ticks 80000000 --start-emission 0
cargo run -p k8dnz-cli -- timemap reconstruct --recipe ./configs/tuned_validated.k8r --timemap /tmp/gen256_rgb_mapped.tm1 --residual /tmp/gen256_rgb_mapped.resid --out /tmp/gen256_rgb_mapped.out --mode rgbpair --map splitmix64 --map-seed 1 --max-ticks 80000000
cmp /tmp/gen256.bin /tmp/gen256_rgb_mapped.out && echo OK
```

---

## Timemap pipeline (bitfield mapping)

### Bitfield(2) example: 4 lanes (00/01/10/11)

This treats the target as a stream of 2-bit symbols and fits time windows that maximize matches.

> Note: when reconstructing, **always pass `--max-ticks` at least as large as the fit run**. We saw `reconstruct short` until we added `--max-ticks 250000000` (and later `--max-ticks 600000000`), after which reconstruction succeeded.

```bash
cargo run -p k8dnz-cli -- timemap fit-xor-chunked \
  --recipe ./configs/tuned_validated.k8r \
  --target text/Genesis1.txt \
  --out-timemap /tmp/g1_bf2.tm1 \
  --out-residual /tmp/g1_bf2.bf2 \
  --mode rgbpair \
  --map bitfield \
  --bits-per-emission 2 \
  --bit-mapping geom \
  --time-split \
  --objective matches \
  --chunk-size 512 \
  --lookahead 1200000 \
  --search-emissions 8000000 \
  --max-ticks 250000000 \
  --zstd-level 3
```

Reconstruct + verify:

```bash
cargo run -p k8dnz-cli -- timemap reconstruct \
  --recipe ./configs/tuned_validated.k8r \
  --timemap /tmp/g1_bf2.tm1 \
  --residual /tmp/g1_bf2.bf2 \
  --out /tmp/g1_bf2.recon \
  --mode rgbpair \
  --map bitfield \
  --bits-per-emission 2 \
  --bit-mapping geom \
  --residual-mode xor \
  --max-ticks 250000000

cmp -s text/Genesis1.txt /tmp/g1_bf2.recon && echo "OK: bitfield(2) reconstruct matches target" || echo "FAIL: mismatch"
```

Inspect lane distribution:

```bash
cargo run -p k8dnz-cli -- timemap bf-lanes --in /tmp/g1_bf2.bf2 --zstd-level 3
```

### Bitfield(2) + hash mapping variant

```bash
cargo run -p k8dnz-cli -- timemap fit-xor-chunked \
  --recipe ./configs/tuned_validated.k8r \
  --target text/Genesis1.txt \
  --out-timemap /tmp/g1_bf2_hash.tm1 \
  --out-residual /tmp/g1_bf2_hash.bf2 \
  --mode rgbpair \
  --map bitfield \
  --bits-per-emission 2 \
  --bit-mapping hash \
  --map-seed 1337 \
  --time-split \
  --objective matches \
  --chunk-size 512 \
  --lookahead 1200000 \
  --search-emissions 8000000 \
  --max-ticks 250000000 \
  --zstd-level 3
```

Reconstruct (note the explicit `--max-ticks` again):

```bash
cargo run -p k8dnz-cli -- timemap reconstruct \
  --recipe ./configs/tuned_validated.k8r \
  --timemap /tmp/g1_bf2_hash.tm1 \
  --residual /tmp/g1_bf2_hash.bf2 \
  --out /tmp/g1_bf2_hash.recon \
  --mode rgbpair \
  --map bitfield \
  --bits-per-emission 2 \
  --bit-mapping hash \
  --map-seed 1337 \
  --residual-mode xor \
  --max-ticks 250000000
```

### Bitfield(1) direction (next experiment)

The next planned experiment is 1-bit emissions (“color pair = 0/1”) with a larger tick budget.

```bash
cargo run -p k8dnz-cli -- timemap fit-xor-chunked \
  --recipe ./configs/tuned_validated.k8r \
  --target text/Genesis1.txt \
  --out-timemap /tmp/g1_bf1.tm1 \
  --out-residual /tmp/g1_bf1.bf2 \
  --mode rgbpair \
  --map bitfield \
  --bits-per-emission 1 \
  --bit-mapping geom \
  --time-split \
  --objective matches \
  --chunk-size 512 \
  --lookahead 1200000 \
  --search-emissions 8000000 \
  --max-ticks 250000000 \
  --zstd-level 3
```

Then:

```bash
cargo run -p k8dnz-cli -- timemap reconstruct \
  --recipe ./configs/tuned_validated.k8r \
  --timemap /tmp/g1_bf1.tm1 \
  --residual /tmp/g1_bf1.bf2 \
  --out /tmp/g1_bf1.recon \
  --mode rgbpair \
  --map bitfield \
  --bits-per-emission 1 \
  --bit-mapping geom \
  --residual-mode xor \
  --max-ticks 250000000
```

Inspect lanes:

```bash
cargo run -p k8dnz-cli -- timemap bf-lanes --in /tmp/g1_bf1.bf2 --zstd-level 3
```

---

## Orbital experiments (`orbexp`)

### Blockscan (diagnostic): derive meet-time stats

```bash
cargo run -p k8dnz-cli -- orbexp blockscan \
  --in text/Genesis1.txt \
  --block-bits 128 \
  --mod 4294967296 \
  --limit 256
```

### Bandsplit (primitive): deterministic lanes + TG1 tags (recommended MOD)

```bash
cargo run -p k8dnz-cli -- orbexp bandsplit \
  --in text/Genesis1.txt \
  --out-prefix /tmp/ts_mod_m1 \
  --block-bits 128 \
  --mod 4294967295 \
  --bucket-fn tfirst \
  --bucket-shift 29 \
  --bucket-mod 4 \
  --emit-tags \
  --tag-format packed \
  --tag-bits 2
```

This emits:

* `/tmp/ts_mod_m1.data.bin` (adjacency-preserving block stream)
* `/tmp/ts_mod_m1.tags.bin` (TG1 packed tags)

---

## Artifact format: `.ark` (binary)

Current `.ark` is a deterministic container:

```
MAGIC[4] = "ARK1"
recipe_len:u32
recipe_bytes[recipe_len]      (K8R recipe blob, includes its own checks)
data_len:u64
data_bytes[data_len]          (ciphertext)
crc32:u32                     (over everything before crc32)
```

The `.ark` file is self-contained:

* embedded recipe enables deterministic regen of the keystream
* `decode` recomputes the keystream and XORs back to plaintext
* CRC guards corruption

---

## “ARK Key” formats (future-facing)

We expect to support a compact **string key** that can reproduce outputs without shipping a full recipe file. These are sketches (not final), but they match the project direction.

### Option 1: URL-safe ARK string (human-copyable)

```
ARK1:
A=<packed orbit A + seed>
B=<field seed + mapping params (clamp/quant/shift + optional permutation)>
C=<packed orbit C + seed>
M=<mode (bytes/text/pages)>
L=<length target>
H=<checksum/CRC>
```

### Option 2: “short form” key (minimal)

```
ARK1:<recipe_id_hex>:<mode>:<length>:<crc>
```

### Option 3: Packed + Base32/Base64url (ultra-compact)

```
ARK1_<base64url(packed_bytes)>
```

### Option 4: “Russian doll pages” (cascading composition)

Pages are first-class outputs:

* output is “page chunks” that can be chained
* pages can themselves contain ARK strings (cascading composition)

Note: cascading compression will likely be built last.

---

## Why “time” matters (the project’s core invariant)

Cadence output is indexed by deterministic tick time. The same recipe + seed produces the same emissions at the same ticks.
If the time index is off—even slightly—the emitted pairs differ or disappear.

This property enables:

* reproducible regen
* “counting game” reconstruction
* curve/arc decoding ideas (paired digits → numbers → curves → recover pairs with known time)
* the timemap+residual “double helix” reconstruction layer

---

## Roadmap (next milestones)

### Near-term (now)

* Fix recipe tuning degeneracy: ensure any tuned recipe generates a non-degenerate stream (no “all zeros” regen)
* Residual-first optimization: update timemap objective from “max matches” to “min zstd(residual)” (true compression objective)
* Add more mapping families beyond SplitMix64 (affine byte map, permute-256 table from seed, lane-aware mapping for rgbpair)
* Improve scoreboard reporting:

  * recipe bytes
  * timemap bytes (and compressed timemap bytes)
  * residual zstd bytes
  * total effective bytes vs plain zstd

### Next experiment set (active)

* Conditioning V2 (core engineering milestone):

  * replace conditioning-as-XOR-mask (falsified) with conditioning where TG1 tags select:

    * mapping/field seed variants, or
    * mapping families, or
    * timeline-specific scan/mapping rules
  * success criterion: residual zstd decreases vs baseline (even 5–15% is signal)

* Time-split into 4 timelines:

  * 00 / 01 / 10 / 11 via 2-bit TG1 tags or via bitfield(2) lanes
  * model each timeline with its own deterministic mapping/field variant
  * goal: push more structure into the “program” (TM1/tags) so residual shrinks

* 1-bit emission direction:

  * “color pair = 0/1” as the primitive token and/or 1-bit tags
  * aim: maximize structure in the timing/banding program so residual shrinks

### Mid-term

* Compress TM1 efficiently (delta + varint + zstd)
* Multi-window / piecewise TM1 selection to reduce residual
* ARK string spec + decoder (embed mapping + TM1 + residual in a compact form)

### Long-term

* End-to-end: KJV Bible as reproducible output via ARK keys
* Cascading ARK pages (Russian doll composition)
* Deterministic visualization tooling (optional π/τ only)

---

## License

MIT OR Apache-2.0

```
```
