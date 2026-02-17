# K8DNZ / The Cadence Project (Rust)

>Update: Major design flaw discovered for arkc (/merkel/) - Each row in the merkel tree is being stacked together then compressed which is not the intended design - its showing much higher data output - patch in progress 




# How to run:

```bash
cargo build --release

INPUT_PATH="path/to/input.bin"
OUT_ROOT="/tmp/output.arkm"
OUT_RECON="/tmp/output.reconstructed"

target/release/arkc "$INPUT_PATH" --out "$OUT_ROOT" --chunk-bytes 2048
target/release/arku "$OUT_ROOT" "$OUT_RECON"

cmp -s "$INPUT_PATH" "$OUT_RECON" && echo OK || echo MISMATCH
```

# How to run example:

```bash
cargo build --release

target/release/arkc text/TheBookOfGenesis.txt --out /tmp/genesis_full.arkm --chunk-bytes 2048
target/release/arku /tmp/genesis_full.arkm /tmp/genesis_full.unpacked

cmp -s text/TheBookOfGenesis.txt /tmp/genesis_full.unpacked && echo OK || echo MISMATCH
```


## How `arkc` / `arku` runs (Merkle-encoded ARK)

> Example input: `text/TheBookOfGenesis.txt`

K8DNZ encodes data into a **Merkle tree of deterministic “node blobs.”** Each blob is a self-contained program:
- a compact **timemap** (the “time program”)
- a **residual** (xor/sub, etc.)
- the **recipe + recon params** needed to deterministically reconstruct bytes

### Phase 1: Leaves (chunk compression)

`arkc` splits the input into fixed-size chunks (`--chunk-bytes`, e.g. 2048), then compresses each chunk as a **leaf blob**:

- It processes `LEAF 1/128 … LEAF 128/128`
- Most leaves are exactly `chunk_bytes` (2048)
- The last real leaf may be smaller if the file size isn’t a multiple of `chunk_bytes`
- Leaves are padded up to a power-of-two (`padded_leaves`) so the Merkle tree is complete

You’ll see logs like:

- `real_leaves=121 padded_leaves=128 (pow2)`
- `LEAF 1/128 bytes=2048 seed=...`
- `reconstruct ok ...`
- `success: reconstruction matches payload`

### Phase 2: Internal nodes (parent compression)

After all leaves are produced, `arkc` builds the Merkle tree **bottom-up**. Each internal node payload is formed by combining two child blobs (left + right) into a deterministic parent payload, then compressing that payload into its own node blob.

For `padded_leaves=128`, internal levels are:

- Level above leaves: 64 nodes  
- Next: 32  
- Next: 16  
- Next: 8  
- Next: 4  
- Next: 2  
- Root: 1  

Total internal nodes = **127**.

This continues until the **root blob** is produced. The root path you pass to `--out` is the root of the Merkle-ARK container.

---

## How it “decompresses” (reconstruction)

`arku` reconstructs the original file by walking the Merkle-ARK container and rebuilding the tree **top-down**:

1. Start at the **root blob**
2. Deterministically **reconstruct** the root payload (timemap + residual + recipe params)
3. From that payload, obtain the two child blobs (left/right)
4. Repeat until reaching the **leaf blobs**
5. Reconstruct each leaf’s original chunk bytes and write them out **in order**
6. Trim padding / truncate to the stored original length

Because reconstruction is deterministic, **any mismatch means something is wrong** (params, residual parsing, or determinism), and `cmp` should catch it.

### Example

```bash
target/release/arkc text/TheBookOfGenesis.txt --out /tmp/genesis_full.arkm --chunk-bytes 2048
target/release/arku /tmp/genesis_full.arkm /tmp/genesis_full.unpacked
cmp -s text/TheBookOfGenesis.txt /tmp/genesis_full.unpacked && echo OK || echo MISMATCH
```

**Last updated:** 2026-02-16
**Status:** Experimental — but we now have a proven “time-indexed reconstruction” pipeline, a vision-aligned structured-bit mapping (`lowpass-thresh`), and (new) a **byte-perfect merkle-style cascading compression + decompression proof** (“merkle-zip / merkle-unzip”). This establishes the core closure property we need for ARK-of-ARKs composition.

## Current progress (2026-02-16)

We have a new, concrete “vision-aligned” path that is showing real signal:

* ✅ `timemap gen-law` now generates TM1 + residual from a deterministic **law** (no window-scanning required).
* ✅ A new bitfield mapping family, **low-pass threshold** (`--bit-mapping lowpass-thresh`), produces **structured bits** (runs/streaks) so the residual becomes more zstd-friendly.
* ✅ We reached **57.31% matches** on a 1024-byte target (bitfield(1), rgbpair, lowpass-thresh, closed-form law).
* ✅ On a 512-byte target, we got **effective_no_recipe = 303 vs plain_zstd = 272** (**only +31 bytes** overhead), meaning we are **closing in on plain zstd** while still using a deterministic “program + patch” architecture.
* ✅ **NEW (major milestone):** we proved **merkle-style cascade compression and decompression** is byte-perfect. Concretely: we built two “leaf ARK blobs” (recipe + timemap + residual) for two halves of data, packed them into a container, compressed that container into a single “root ARK blob,” then decompressed the root and recovered the original leaf blobs **bit-for-bit identical**.

### Current formula (math)

We are currently using **law-driven indexing + low-pass threshold bit extraction + XOR residual**.

**Law-driven index selection (contiguous TM1 window)**

$$
\mathrm{TM1}[i] = s + i,\quad i = 0,1,\ldots,N-1
$$

where $s$ (the chosen start) is produced deterministically by the selected law (`jump-walk` or `closed-form`) over the legal window length $W$.

**Low-pass threshold bit extraction**

$$
\hat{x}[t] = \mathbb{1}!\left(\mathrm{LP}!\left(I(t);\tau,\mathrm{smooth_shift}\right) \ge \theta\right)
$$

(`lowpass-thresh` smooths a deterministic intensity signal from the generator and thresholds it into 0/1.)

**Residual (XOR)**

$$
r[i] = x[i] \oplus \hat{x}!\left(\mathrm{TM1}[i]\right)
$$

> Important: match% is not the only metric; we care about **residual compressibility**. Low-pass helps even when match% is moderate because it makes the residual less noise-like.

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
  * a **residual** (patch bytes; XOR and SUB modes; plus bitfield residual payloads in fit paths)

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
* timemap runs print a **scoreboard** (recipe bytes, TM bytes/zstd, residual bytes/zstd, effective totals)

---

## NEW milestone: Merkle-style cascading compression is proven (merkle-zip / merkle-unzip)

This is the “moment of truth” milestone for cascading composition.

**Claim:** We can take two leaf “ARK artifacts” (recipe + timemap + residual) and compress them into a single root “ARK artifact” (recipe + timemap + residual). Then we can decompress the root and recover the two leaf artifacts **byte-for-byte identical**.

This is exactly what we proved using:

* leaf mapping: `bitfield(1) + rgbpair + lowpass-thresh`
* residual mode: `xor`
* tick budget: `--max-ticks 200000000`
* a tiny packed container format `"K8P2"` (magic + version + lengths + payload)

### What the artifacts were (concrete)

We built two leaf blobs:

* `A.blob = recipe.k8r || a_full.tm1 || a_full.bf`
* `B.blob = recipe.k8r || b_full.tm1 || b_full.bf`

Then we packed them into:

* `P.bin = K8P2 container that contains A.blob and B.blob`

Then we compressed P.bin into a root artifact:

* `root.tm1` + `root.bf` (with the same recipe and same mapping params)

Then we decompressed root back into:

* `P.out` and verified `P.out == P.bin`

Then we unpacked `P.out` back into:

* `A2.blob` and `B2.blob` and verified `A2.blob == A.blob` and `B2.blob == B.blob`

### Size results (from the proven run)

* `A.blob` = 2343 bytes
* `B.blob` = 2344 bytes
* `P.bin` = 4700 bytes

Root “ARK artifact” size (compressed side-data + recipe):

* `recipe_raw_bytes = 210`
* `tm_zstd_bytes = 18`
* `resid_zstd_bytes = 1632`
* `effective_bytes_with_recipe = 1860 bytes`

So the root (1860B) expands into a packed payload (4700B) that contains two leaf artifacts.

### How to reproduce the merkle proof exactly

This is a fully reproducible command sequence.

#### 0) Prepare the two halves

This assumes you already created `/tmp/k8dnz_half/g1_a.bin` and `/tmp/k8dnz_half/g1_b.bin` (split Genesis1 into two parts). If you used a different split method previously, keep using it. The proof does not depend on how you split, only that you keep the same two inputs through the steps below.

#### 1) Generate the two leaf timemap + residual pairs (leaf ARKs)

```bash
cargo run -p k8dnz-cli -- timemap fit-xor-chunked --recipe ./configs/tuned_validated.k8r --target /tmp/k8dnz_half/g1_a.bin --out-timemap /tmp/k8dnz_half/a_full.tm1 --out-residual /tmp/k8dnz_half/a_full.bf --map bitfield --mode rgbpair --bits-per-emission 1 --bit-mapping lowpass-thresh --bitfield-residual packed --residual xor --objective zstd --zstd-level 3 --lookahead 400000 --max-ticks 200000000

cargo run -p k8dnz-cli -- timemap fit-xor-chunked --recipe ./configs/tuned_validated.k8r --target /tmp/k8dnz_half/g1_b.bin --out-timemap /tmp/k8dnz_half/b_full.tm1 --out-residual /tmp/k8dnz_half/b_full.bf --map bitfield --mode rgbpair --bits-per-emission 1 --bit-mapping lowpass-thresh --bitfield-residual packed --residual xor --objective zstd --zstd-level 3 --lookahead 400000 --max-ticks 200000000
```

These runs produce:

* `/tmp/k8dnz_half/a_full.tm1` and `/tmp/k8dnz_half/a_full.bf`
* `/tmp/k8dnz_half/b_full.tm1` and `/tmp/k8dnz_half/b_full.bf`

#### 2) Build the two leaf blobs (leaf ARK blobs)

```bash
mkdir -p /tmp/k8dnz_half/ark2
cp ./configs/tuned_validated.k8r /tmp/k8dnz_half/ark2/recipe.k8r
cp /tmp/k8dnz_half/a_full.tm1 /tmp/k8dnz_half/ark2/a.tm1
cp /tmp/k8dnz_half/a_full.bf  /tmp/k8dnz_half/ark2/a.bf
cp /tmp/k8dnz_half/b_full.tm1 /tmp/k8dnz_half/ark2/b.tm1
cp /tmp/k8dnz_half/b_full.bf  /tmp/k8dnz_half/ark2/b.bf

cat /tmp/k8dnz_half/ark2/recipe.k8r /tmp/k8dnz_half/ark2/a.tm1 /tmp/k8dnz_half/ark2/a.bf > /tmp/k8dnz_half/ark2/A.blob
cat /tmp/k8dnz_half/ark2/recipe.k8r /tmp/k8dnz_half/ark2/b.tm1 /tmp/k8dnz_half/ark2/b.bf > /tmp/k8dnz_half/ark2/B.blob

wc -c /tmp/k8dnz_half/ark2/A.blob /tmp/k8dnz_half/ark2/B.blob
```

Expected sizes from the proven run were:

* `A.blob = 2343`
* `B.blob = 2344`

#### 3) Pack the two leaf blobs into one self-delimiting container (K8P2)

```bash
LA=$(wc -c < /tmp/k8dnz_half/ark2/A.blob)
LB=$(wc -c < /tmp/k8dnz_half/ark2/B.blob)

printf 'K8P2' > /tmp/k8dnz_half/ark2/P.bin
printf '\x01' >> /tmp/k8dnz_half/ark2/P.bin
perl -e 'print pack("V",$ARGV[0])' "$LA" >> /tmp/k8dnz_half/ark2/P.bin
perl -e 'print pack("V",$ARGV[0])' "$LB" >> /tmp/k8dnz_half/ark2/P.bin
cat /tmp/k8dnz_half/ark2/A.blob /tmp/k8dnz_half/ark2/B.blob >> /tmp/k8dnz_half/ark2/P.bin

wc -c /tmp/k8dnz_half/ark2/P.bin
```

Expected from the proven run:

* `P.bin = 4700`

#### 4) merkle-zip: compress the packed container into a single root timemap+residual

```bash
cargo run -p k8dnz-cli -- timemap fit-xor-chunked --recipe ./configs/tuned_validated.k8r --target /tmp/k8dnz_half/ark2/P.bin --out-timemap /tmp/k8dnz_half/ark2/root.tm1 --out-residual /tmp/k8dnz_half/ark2/root.bf --map bitfield --mode rgbpair --bits-per-emission 1 --bit-mapping lowpass-thresh --bitfield-residual packed --residual xor --objective zstd --zstd-level 3 --lookahead 400000 --max-ticks 200000000
```

This produces the root artifact:

* `/tmp/k8dnz_half/ark2/root.tm1`
* `/tmp/k8dnz_half/ark2/root.bf`

#### 5) merkle-unzip: reconstruct P.out from the root artifact

Important: `timemap reconstruct` uses `--residual-mode` and does not accept `--bitfield-residual`.

```bash
cargo run -p k8dnz-cli -- timemap reconstruct --recipe ./configs/tuned_validated.k8r --timemap /tmp/k8dnz_half/ark2/root.tm1 --residual /tmp/k8dnz_half/ark2/root.bf --out /tmp/k8dnz_half/ark2/P.out --map bitfield --mode rgbpair --bits-per-emission 1 --bit-mapping lowpass-thresh --residual-mode xor --bit-tau 128 --bit-smooth-shift 3 --max-ticks 200000000
```

Verify byte identity:

```bash
cmp -s /tmp/k8dnz_half/ark2/P.bin /tmp/k8dnz_half/ark2/P.out && echo OK_P
```

Expected:

* `OK_P`

#### 6) Unpack P.out and verify both recovered leaf blobs match exactly

```bash
MAGIC=$(dd if=/tmp/k8dnz_half/ark2/P.out bs=1 count=4 status=none)
test "$MAGIC" = "K8P2" && echo OK_MAGIC

LA=$(dd if=/tmp/k8dnz_half/ark2/P.out bs=1 skip=5 count=4 status=none | perl -e 'read(STDIN,$x,4); print unpack("V",$x)')
LB=$(dd if=/tmp/k8dnz_half/ark2/P.out bs=1 skip=9 count=4 status=none | perl -e 'read(STDIN,$x,4); print unpack("V",$x)')

dd if=/tmp/k8dnz_half/ark2/P.out bs=1 skip=13 count=$LA status=none > /tmp/k8dnz_half/ark2/A2.blob
dd if=/tmp/k8dnz_half/ark2/P.out bs=1 skip=$((13+LA)) count=$LB status=none > /tmp/k8dnz_half/ark2/B2.blob

cmp -s /tmp/k8dnz_half/ark2/A.blob /tmp/k8dnz_half/ark2/A2.blob && echo OK_A_BLOB
cmp -s /tmp/k8dnz_half/ark2/B.blob /tmp/k8dnz_half/ark2/B2.blob && echo OK_B_BLOB
```

Expected:

* `OK_MAGIC`
* `OK_A_BLOB`
* `OK_B_BLOB`

This completes the merkle-style proof: **root expands to the exact packed container that contains exact leaf artifacts**.

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
* **Bitfield(1)** is structurally interesting: match rates jump dramatically (often ~55–60% matches per 512-symbol chunk), suggesting the generator is “closer” to a 1-bit view.

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

* Under `mod = 2^32 (4294967296)`, lane assignment can become degenerate.
* Under `mod = 2^32 - 1 (4294967295)`, the same setup produces all 4 lanes with meaningful distribution.

Interpretation:

* This is not invertible decoding (collisions are expected and fine).
* But it is a powerful deterministic primitive.

---

## What we’re building (end product)

The end product is a **deterministic expansion key system**:

> A compact ARK key (recipe + seeds + mapping rules + length + checksum) deterministically regenerates large outputs—pages of text, bytes, or structured tokens—without needing the original input file.

This has two related goals:

1. **Deterministic expansion** (already real): a short key can generate large reproducible streams.
2. **Compression-by-model** (actively underway): for structured data (e.g., text), fit a cadence recipe + mapping + timing map so that the target can be reconstructed with a **small, compressible residual**.

We are currently validating the system end-to-end using **Genesis1.txt** as the canonical sample.

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

* `/tmp/ts_mod_m1.data.bin`
* `/tmp/ts_mod_m1.tags.bin`

---

## Artifact format: `.ark` (binary)

Current `.ark` is a deterministic container:

```
MAGIC[4] = "ARK1"
recipe_len:u32
recipe_bytes[recipe_len]
data_len:u64
data_bytes[data_len]
crc32:u32
```

---

## “ARK Key” formats (future-facing)

We expect to support a compact **string key** that can reproduce outputs without shipping a full recipe file.

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

Pages are first-class outputs.

---

## Why “time” matters (the project’s core invariant)

Cadence output is indexed by deterministic tick time. The same recipe + seed produces the same emissions at the same ticks.

---

## Roadmap (next milestones)

### Near-term (now)

* Fix recipe tuning degeneracy: ensure any tuned recipe generates a non-degenerate stream
* Residual-first optimization: objective should prioritize `zstd(residual)` directly (true compression objective)
* Improve scoreboard reporting and add residual run statistics for bitfield(1) / lowpass-thresh

### Next experiment set (active)

* Conditioning V2: replace conditioning-as-XOR-mask with conditioning where TG1 tags select mapping/field variants
* Time-split into 4 timelines: 00 / 01 / 10 / 11
* 1-bit emission direction: maximize structure so residual shrinks

### Mid-term

* Compress TM1 efficiently (delta + varint + zstd)
* Multi-window / piecewise TM1 selection to reduce residual
* Formalize leaf container format (self-describing “leaf ARK blob”) so merkle-zip/unzip never relies on fixed recipe/tm sizes

### Long-term

* End-to-end: larger Genesis samples, then full Book of Genesis, then beyond
* Cascading ARK pages (Russian doll composition)
* Deterministic visualization tooling (optional π/τ only)

---

## License

MIT OR Apache-2.0

```
```
