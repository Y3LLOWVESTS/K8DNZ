````md
# K8DNZ / The Cadence Project (Rust)

**This is in the experimental phase right now**
**The cascading compression feature will probably be built last (ARK keys that unlock other ARK keys that unlock other ARK keys and so forth**)

A deterministic, modular codec prototype inspired by a simple but powerful mechanism:

- Two “dots” (A and C) free-orbit on equal-circumference circles in opposite directions at different speeds.
- When their phases align within a window ε, they enter **lockstep** on a truncated cone (frustum) whose small rim is B.
- In lockstep, the pair moves in perfect tandem on opposite sides (Δ = 0.5 turns by default), spiraling to the large rim.
- At the rim, we sample a deterministic **field**, emit a **paired token**, and the next cycle begins.

The core output is a time-sensitive token stream: if the “time” (tick index) is off, the emitted pairs change—or don’t appear. This time determinism is the backbone of the project’s long-term “ARK key” vision.

---

## Status (what works today)

✅ **Determinism and round-trip are proven**
- `encode` → produces a `.ark` artifact that embeds the recipe and ciphertext
- `decode` → reproduces the original bytes exactly
- tests confirm:
  - **Genesis1.txt roundtrip matches bytes**
  - **two identical runs produce byte-identical `.ark`**

✅ **Simulator is stable**
- pair tokens (`--mode pair`)
- rgb pair tokens (`--mode rgbpair`)
- field-driven RGB (`--rgb-from-field`) with two backends:
  - `dna` and `cone` are **deterministic** and now intentionally **diverge** while still “resonating” at predictable indices

✅ **Instrumentation added**
- `ark-inspect --dump-ciphertext` extracts ciphertext for analysis
- `analyze` prints byte histograms + entropy for any file
- `encode --dump-keystream` dumps raw keystream bytes used for XOR (for generator-quality diagnostics)

---

## What we’re building (end product)

The end product is a **deterministic expansion key system**:

> A compact ARK key (recipe + seeds + mapping rules + length + checksum) deterministically regenerates large outputs—pages of text, bytes, or structured tokens—without needing the original input file.

This has two related goals:

1) **Deterministic expansion** (already proven): a short key can generate large reproducible streams.
2) **Compression-by-model** (in progress): for structured data (e.g., text), fit a cadence recipe + mapping so that the output matches the target with a small residual/patch stream. This is where “small key → large exact text” becomes real compression.

We’re currently validating the system end-to-end using **Genesis1.txt** as the canonical sample. (We scale to more text only after the pipeline is proven on Genesis.)

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
A key design knob is `quant.shift`—it moves bin boundaries without altering cadence timing. In practice it improves symbol/byte distribution.
- `tuned` default uses `qshift=7_141_012`
- `baseline` uses `qshift=0`

### Two “output layers”
- **PairToken layer** (N=16 symbols): stable, compact, great for token pipelines.
- **RGBPair layer**: either palette-mapped from PairTokens or **field-driven** for “cone / helix / DNA” visualization laws.

---

## Repo layout (high level)

- `crates/k8dnz-core/` — deterministic cadence engine, field model, recipe, token types
- `crates/k8dnz-cli/`  — simulator, encoder/decoder, inspect/analyze tools
- `text/Genesis1.txt`  — canonical sample input used for experiments/tests

---

## Quickstart

### Build
```bash
cargo build
````

### Run simulator (pair tokens)

```bash
cargo run -p k8dnz-cli -- sim --emissions 10 --mode pair --fmt jsonl
```

### Run simulator (rgbpair, palette mapping)

```bash
cargo run -p k8dnz-cli -- sim --emissions 10 --mode rgbpair --fmt jsonl
```

### Run simulator (rgbpair from emission-time field, DNA vs Cone)

```bash
cargo run -p k8dnz-cli -- sim --emissions 10 --mode rgbpair --fmt jsonl --rgb-from-field --rgb-backend dna
cargo run -p k8dnz-cli -- sim --emissions 10 --mode rgbpair --fmt jsonl --rgb-from-field --rgb-backend cone
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

### Inspect .ark (verify CRC, embedded recipe ID) + dump ciphertext

```bash
cargo run -p k8dnz-cli -- ark-inspect --in /tmp/genesis1.ark --dump-ciphertext /tmp/genesis1.cipher.bin
```

### Analyze bytes (entropy + top bytes)

```bash
cargo run -p k8dnz-cli -- analyze --in /tmp/genesis1.cipher.bin --top 16
cargo run -p k8dnz-cli -- analyze --in text/Genesis1.txt --top 16
```

### Dump & analyze the raw keystream used for XOR

```bash
cargo run -p k8dnz-cli -- encode \
  --in text/Genesis1.txt \
  --out /tmp/genesis1.ark \
  --profile tuned \
  --max-ticks 50000000 \
  --dump-keystream /tmp/genesis1.keystream.bin

cargo run -p k8dnz-cli -- analyze --in /tmp/genesis1.keystream.bin --top 16
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
B=<field seed + mapping params (clamp/quant/shift + optional permutation)>
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

This assumes:

* `recipe_id_hex` resolves to a known canonical recipe profile, or
* a deterministic derivation from seed + profile is standardized.

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
* mode + length
* checksum/CRC

### Option 4: “Russian doll pages”

A key that targets **pages** as first-class output:

* output is “page chunks” that can be chained
* pages can themselves contain ARK strings (cascading composition)

---

## Why “time” matters (the project’s core invariant)

Cadence output is indexed by deterministic tick time. The same recipe + seed produces the same emissions at the same ticks.
If the time index is off—even slightly—the emitted pairs differ or disappear.

This property enables:

* reproducible regen
* “counting game” reconstruction
* later curve/arc decoding ideas (paired digits → numbers → curves → recover pairs with known time)

---

## Roadmap (next milestones)

### Near-term

* Optional keystream mixing/whitening (opt-in) to improve byte distribution when desired
* “Fit + residual” MVP: generate candidate output and store only residual differences for Genesis1.txt
* Standardize recipe schema (single canonical runtime recipe + optional wire/serde wrapper)

### Mid-term

* Page mode + structured outputs
* ARK string spec + decoder
* Curve/arc reconstruction research layer (“double helix” concept formalization)

### Long-term

* End-to-end: KJV Bible as reproducible output via ARK keys
* Cascading ARK pages (Russian doll composition)
* Deterministic visualization tooling (optional π/τ only)

---

## License

TBD (choose MIT OR Apache-2.0 when publishing if you want a permissive default).

```

If you want, I can also generate:
- a short “GitHub Release Notes” blurb,
- a crisp repo description + topics list,
- and a `LICENSE` + `CONTRIBUTING.md` starter that matches your “deterministic + modular + tests” culture.
```
