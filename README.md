
# K8DNZ / The Cadence Project (Rust)

**Status: Experimental, but we now have a proven “time-indexed reconstruction” pipeline.**

K8DNZ is a deterministic codec prototype inspired by a simple but powerful mechanism:

- Two “dots” (A and C) free-orbit on equal-circumference circles in opposite directions at different speeds.
- When their phases align within a window ε, they enter **lockstep** on a truncated cone (frustum) whose small rim is B.
- In lockstep, the pair moves in perfect tandem on opposite sides (Δ = 0.5 turns by default), spiraling to the large rim.
- At the rim, we sample a deterministic **field**, emit a deterministic token, and the next cycle begins.

The core output is a time-sensitive token stream: if the “time” (tick index) is off, emissions change—or don’t appear. That “time determinism” is the backbone of the project’s long-term **ARK key** vision.

---

## Status (what works today)

### ✅ Deterministic round-trip is proven (ARK container)
- `encode` → produces a `.ark` artifact that embeds the recipe and ciphertext
- `decode` → reproduces the original bytes exactly
- tests confirm:
  - **Genesis1.txt roundtrip matches bytes**
  - **two identical runs produce byte-identical `.ark`**

### ✅ Timemap + Residual reconstruction is proven (the “impossible → possible” milestone)
We now have a second, more important proof:

> Using only a recipe + a timing map (TM1) + a residual patch, we can reconstruct target bytes exactly via deterministic regeneration.

This is implemented as:

- `timemap fit-xor` → finds a window in the generated stream and produces:
  - a **TM1** (time positions / indices into the stream)
  - a **residual** (XOR patch bytes)
- `timemap reconstruct` → regenerates the stream at those indices and XORs the residual to produce the exact target

This has been validated with `cmp ... OK` on:
- 57 bytes (Genesis1 first line)
- 256 bytes (first 256 bytes of Genesis1)
- both `pair` and `rgbpair` modes

This is the current “core win”: deterministic time indexing + small side-data makes perfect reconstruction possible.

### ✅ RGB stream extraction + indexing semantics are correct
We support two byte-stream views:

- **Pair stream**: `tok.pack_byte()` → 1 byte per emission
- **RGBPair stream**: `tok.to_rgb_pair().to_bytes()` → 6 bytes per emission (flattened)

Important indexing rule (now enforced):

- `pair` mode positions: `pos == emission_index`
- `rgbpair` mode positions: `pos == emission_index*6 + lane` (flattened byte stream)

This allows TM1 to reference *exact byte positions* even in rgbpair mode.

### ✅ Optional mapping/permutation layer exists and is reversible
We can optionally apply a mapping layer (e.g., `SplitMix64`) during `fit-xor` and `reconstruct`.

This adds a new degree of freedom:
- it can reshuffle or transform stream bytes deterministically
- we still reconstruct exactly as long as decode uses the same mapping parameters

This is currently a lever for future residual reduction.

### ✅ Instrumentation exists for analysis and diagnostics
- `analyze` prints byte histograms + entropy + zstd ratio
- `ark-inspect --dump-ciphertext` extracts ciphertext for analysis
- `encode --dump-keystream` dumps the keystream used for XOR

---

## What we’re building (end product)

The end product is a **deterministic expansion key system**:

> A compact ARK key (recipe + seeds + mapping rules + length + checksum) deterministically regenerates large outputs—pages of text, bytes, or structured tokens—without needing the original input file.

This has two related goals:

1) **Deterministic expansion** (already real): a short key can generate large reproducible streams.
2) **Compression-by-model** (now actively underway): for structured data (e.g., text), fit a cadence recipe + mapping + timing map so that the target can be reconstructed with a **small, compressible residual**.

We are currently validating the system end-to-end using **Genesis1.txt** as the canonical sample.
We scale to larger text only after the pipeline is proven on Genesis.

---

## Core concepts

### Fixed-point “turns” (no floats)
All phase/time evolution uses integer fixed-point turns. No π in core logic. (π/τ is optional for visualization only.)

### Deterministic field sampling
At emission time, we sample a deterministic field model at known coordinates derived from:
- lockstep phase (phi_l)
- paired phase (phi_l + Δ)
- axial parameter (t at the rim)
- engine time (ticks)

Then we clamp + quantize deterministically to emit tokens.

### Quant shift as a distribution knob
A key design knob is `quant.shift`—it moves bin boundaries without altering cadence timing.

**Important update:** we observed that certain “tuned” recipes can become *degenerate* (regen produces all-zero tokens/bytes), while validated configs produce healthy entropy. This is now a tracked issue: **recipe tuning must never produce a degenerate stream**.

### Two output layers
- **PairToken layer**: compact and stable for token pipelines; packable to 1 byte per emission.
- **RGBPair layer**: 6 bytes per emission; can be palette-mapped or field-driven, and is now fully supported by TM1 via flattened indexing.

### The “Double Helix” viewpoint (where the project direction crystallized)
A successful reconstruction can be thought of as two interlocking strands:

- **Strand 1 (Time / Index / Curve):** the TM1 timing map – which positions matter
- **Strand 2 (Value / Patch):** the residual – what must be XOR’d at those positions

Together, they reconstruct the target perfectly from a deterministic generator.
The major remaining goal is to make these strands **small** (especially the residual).

---

## Repo layout (high level)

- `crates/k8dnz-core/` — deterministic cadence engine, field model, recipe, token types
- `crates/k8dnz-cli/`  — simulator, encoder/decoder, timemap tools, inspect/analyze tools
- `text/Genesis1.txt`  — canonical sample input used for experiments/tests

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

A structured key that carries just enough to regenerate:

```
ARK1:
A=<packed orbit A + seed>
B=<field seed + mapping params (clamp/quant/shift + mapping/permutation)>
C=<packed orbit C + seed>
M=<mode: bytes|text|pages>
L=<length target / pages>
H=<checksum>
```

### Option 2: “short form” key (minimal)

A compact “recipe-id + overrides” style:

```
ARK1:<recipe_id_hex>:<mode>:<length>:<crc>
```

### Option 3: Packed + Base32/Base64url (ultra-compact)

Binary-packed fields + checksum, encoded as a short string:

```
ARK1_<base64url(packed_bytes)>
```

Packing candidates:

* version
* seed(s)
* orbit params (fixed-point)
* clamp/quant/shift
* mapping mode + mapping seed
* mode + length
* checksum/CRC

### Option 4: “Russian doll pages” (cascading composition)

Pages are first-class outputs:

* output is “page chunks” that can be chained
* pages can themselves contain ARK strings (cascading composition)

**Note:** cascading compression will likely be built last.

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

* **Fix recipe tuning degeneracy:** ensure any tuned recipe generates a non-degenerate stream (no “all zeros” regen)
* **Residual-first optimization:** update `fit-xor` objective from “max matches” to “min zstd(residual)” (true compression objective)
* Add more mapping families beyond SplitMix64 (affine byte map, permute-256 table from seed, lane-aware mapping for rgbpair)
* Add a “scoreboard” report:

  * recipe bytes
  * timemap bytes (and compressed timemap bytes)
  * residual zstd bytes
  * total effective bytes

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

MIT or Apache-2.0 

```
```
