Yeah — “randomness as a steering wheel” is a real thing, and it fits your vibe of **finding shapes / harmonics** instead of praying the search stumbles into them.

Right now your sweep output is screaming a clue: across EPS / start_emission / choice_rank you’re stuck at **32/64 = 50%** basically every time. That means the current setup (law + lowpass-thresh bitfield + 1b/emission) is behaving like a **coin flip predictor** for that target, i.e. you’re not giving the system a degree-of-freedom that actually *changes the predictive structure*.

What you’re proposing (rate randomness → split/rotate → rate again) is exactly the kind of new degree-of-freedom that can create “shape”.

## Use randomness as a *map* (not noise)

Think of each chunk as having a “texture” that you can measure deterministically:

**Fast, cheap chunk scores (all deterministic):**

* **Shannon entropy** on bytes (or bits)
* **Bit-bias**: `abs(popcount(bits) / nbits - 0.5)`
* **Run-length score** (how “stripey” it is)
* **Autocorrelation** at small lags (1..64) — this is your “harmonic detector”
* **Compressibility proxy**: `zstd(chunk).len / chunk.len` (or even a tiny LZ complexity estimate)

These give each chunk a **feature vector**. Now the key move:

> Use those features to *choose how the engine is applied* (or how data is arranged) so “structured” parts get a deterministic, strong predictor — and “random” parts get handled differently.

## The recursive “entropy-guided rotate” strategy

Here’s a concrete deterministic pipeline that matches what you described:

### 1) Chunk → score → classify

Split into fixed chunks (64B like you’re doing), compute features, then assign each chunk to a class:

* **L (low entropy / high structure)**
* **M (mixed)**
* **H (high entropy / low structure)**

You don’t need ML. Just thresholds.

### 2) Rotate within a window to maximize harmonic alignment

For each chunk (or for a window of several chunks), search a *small* set of rotations/shifts `s` (e.g. 0..63 bytes or 0..511 bits) and pick the one that maximizes a harmonic score:

* `score(s) = corr( chunk_shifted(s), engine_stream_segment )`
* or if you don’t want engine in the loop yet:

  * maximize autocorrelation or minimize entropy after XOR with a simple deterministic mask derived from the recipe

This is how you “find shapes”: you’re literally choosing the phase/offset that makes the data most explainable by a deterministic waveform.

### 3) Re-chunk after rotation (multi-scale)

Now re-chunk at a different scale (64 → 128 → 256) and repeat:

* score again
* rotate again (smaller search)
* stop when improvement flattens

This is basically building a **hierarchy of phase alignments** (your frustum spiral imagery maps to this *perfectly*).

### 4) Feed chunk class into prediction mode selection

Instead of one universal mode (`bitfield + lowpass-thresh + xor residual`), you let the chunk class pick a mode:

* **L chunks:** exploit structure

  * more aggressive deterministic predictor (strong smoothing / larger tau / maybe 2 bits per emission)
* **H chunks:** stop pretending — use a mode that minimizes overhead

  * fewer parameters, or even fall back to “store residual compactly”
* **M chunks:** split lanes (your “lanes” idea) and treat bits differently (bias-aware mapping)

This is where predictability increases: you stop forcing one predictor to fit everything.

## Where this plugs into K8DNZ cleanly

You don’t need to rewrite the universe. Add a **prepass** that outputs a tiny “shape plan”:

**ShapePlan (must be tiny):**

* chunk size(s) used (probably fixed, not stored per chunk)
* per-chunk class in 2 bits (L/M/H) *or* only store exceptions/runs
* per-chunk rotation shift `s` (delta-coded, or only store when non-zero)
* optional: per chunk “mode id” (2–3 bits)

This can be packed insanely small if you run-length encode it.

Then `gen-law` / `fit-xor-chunked` uses ShapePlan to:

* choose start positions / offsets smarter
* choose mapping parameters per chunk class
* optionally restrict search to offsets suggested by “harmonic peaks” (autocorr maxima)

## The “backwards through the engine” harmonic test

If you want the “harmonic when reversed” idea to be a real diagnostic, do this:

1. Generate the engine’s predicted bitstream `P(t)` for a large range once.
2. For the target chunk bits `X`:

   * compute `C(t) = corr( X, P(t..t+n) )` over t in a range
3. Plot or just log the top-k peaks and their spacing.

   * If peak spacings are regular ⇒ you found a harmonic / periodic structure.
   * Then restrict your law search to around those peaks.

That gives you **shape discovery** that is measurable and repeatable.

## Why this helps your “50% wall”

Right now, your parameters aren’t changing the predictor’s relationship to the target — so you get coinflip matches.

The entropy-guided rotate strategy creates *meaningful* variation by:

* forcing phase alignment (which your vision strongly implies)
* separating structured vs noise-like regions (so you don’t average them together)
* choosing predictor style based on measured chunk texture

If you want, the next “minimum patch” direction is:

* add a small CLI tool like `k8dnz-cli analyze-rand --in FILE --chunk 64` that prints chunk features + suggested class
* then add `--shape-plan OUT.plan` to write the compact plan
* then `gen-law` can accept `--shape-plan IN.plan` and use it to adjust start offsets / choose mapping preset per chunk class

That’s the shortest path to turning “randomness” into a deterministic steering signal without blowing up the codebase.
