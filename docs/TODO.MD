k8dnz/
  Cargo.toml
  README.md
  LICENSE-MIT
  LICENSE-APACHE
  .gitignore
  rustfmt.toml
  clippy.toml

  crates/
    k8dnz-core/
      Cargo.toml
      src/
        lib.rs

        error.rs                  # crate Error/Result
        validate.rs               # recipe + runtime param validation helpers

        fixed/
          mod.rs
          turn32.rs               # Turn32 wrapping phase type (u32)
          unit32.rs               # Unit32 saturating progress type (u32)
          math.rs                 # modular distance, wrap helpers

        dynamics/
          mod.rs
          engine.rs               # Engine { state, recipe, stats } + step()
          state.rs                # enum Mode { FreeOrbit, Lockstep } + structs
          free_orbit.rs           # FREE_ORBIT tick + edge-trigger align detect
          lockstep.rs             # LOCKSTEP tick (phiL + t_step) + emit gate
          reset.rs                # ResetMode impls (MVP: HoldAandC)

        field/
          mod.rs
          tri_wave.rs             # deterministic triangle-wave field
          params.rs               # wave component params, integer eval

        signal/
          mod.rs
          sample.rs               # FieldSample type (bounded i32/i64 contract)
          quantize.rs             # sample -> symbol bins (deterministic floor)
          token.rs                # PairToken, pack/unpack to byte for N=16

        recipe/
          mod.rs
          recipe.rs               # Recipe struct (versioned logical config)
          format.rs               # binary-stable encoding layout (owned)
          checksum.rs             # crc32/blake3 helpers
          defaults.rs             # sane default recipe builders

        stats/
          mod.rs
          counters.rs             # align/emission counters, cadence metrics

      tests/
        determinism.rs            # same recipe+seed => same token/byte stream
        invariants.rs             # Δ preserved, vA!=vC, vL same in lockstep
        wrap_math.rs              # modular distance wrap cases
        golden_stream.rs          # snapshot-based regression over fixtures

    k8dnz-cli/
      Cargo.toml
      src/
        main.rs                   # clap root
        cmd/
          mod.rs
          sim.rs                  # run for ticks/emissions; output tokens/bytes
          decode.rs               # regenerate stream from recipe
          tune.rs                 # stub (Deliverable 4 later)
        io/
          mod.rs
          jsonl.rs                # JSONL tokens output (human-friendly)
          bin.rs                  # raw bytes output
          recipe_file.rs          # load/save recipe (binary + optional toml)

  tests/
    cli_smoke.rs                 # runs k8dnz sim/decode with fixtures
    cli_golden.rs                # verifies stable output hashes for recipes

  fixtures/
    recipes/
      default.k8r                # binary recipe fixture (our format)
      fast_align.k8r
    golden/
      default_256.bytes          # expected first 256 bytes
      fast_align_256.bytes

  configs/
    default.toml                 # optional human-editable recipe source
    fast_align.toml

  scripts/
    run_sim.sh
    run_decode.sh
    fmt_clippy_test.sh

  docs/
    NOTES.md                     # carry-over notes (your spec)
    DESIGN.md                    # architecture + invariants
    RECIPE_FORMAT.md             # exact binary layout
    FIELD.md                     # triangle wave + bounds
    TUNING.md                    # future: objective functions + search plan
