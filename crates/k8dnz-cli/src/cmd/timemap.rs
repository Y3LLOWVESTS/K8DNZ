// crates/k8dnz-cli/src/cmd/timemap.rs

use clap::{Args, Subcommand, ValueEnum};
use k8dnz_core::signal::bitpack;
use k8dnz_core::signal::timing_map::TimingMap;
use k8dnz_core::Engine;

use crate::io::{recipe_file, timemap};

use std::convert::TryInto;

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum ApplyMode {
    Pair,
    Rgbpair,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum MapMode {
    None,
    Splitmix64,
    Ascii7,
    Ascii7Splitmix,

    Text40,
    Text40Weighted,

    Text40Lane,
    Text40Field,

    /// NEW: k-bit symbol stream derived from RGBPAIR emissions (vision-aligned "bits via field")
    Bitfield,

    Text64,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum BitMapping {
    Geom,
    Hash,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum FitObjective {
    Matches,
    Zstd,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum ResidualMode {
    Xor,
    Sub,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum SeedFmt {
    Text,
    Json,
}

#[derive(Args)]
pub struct TimemapArgs {
    #[command(subcommand)]
    pub cmd: TimemapCmd,
}

#[derive(Subcommand)]
pub enum TimemapCmd {
    Make(MakeArgs),
    Inspect(InspectArgs),

    /// Decode/inspect packed map_seed fields (decoder ring)
    MapSeed(MapSeedArgs),

    Apply(ApplyArgs),
    Fit(FitArgs),
    FitXor(FitXorArgs),
    FitXorChunked(FitXorChunkedArgs),
    Reconstruct(ReconstructArgs),
}

#[derive(Args)]
pub struct MakeArgs {
    #[arg(long)]
    pub out: String,

    #[arg(long)]
    pub len: u64,

    #[arg(long, default_value_t = 0)]
    pub start: u64,

    #[arg(long, default_value_t = 1)]
    pub step: u64,
}

#[derive(Args)]
pub struct InspectArgs {
    #[arg(long)]
    pub r#in: String,
}

#[derive(Args)]
pub struct MapSeedArgs {
    /// mapping mode to interpret (decoder ring currently defined for text40-field)
    #[arg(long, value_enum, default_value_t = MapMode::Text40Field)]
    pub map: MapMode,

    /// seed as decimal u64
    #[arg(long, default_value_t = 0)]
    pub map_seed: u64,

    /// seed as hex (accepts "0x..." or raw hex). If set, overrides --map-seed.
    #[arg(long)]
    pub map_seed_hex: Option<String>,

    #[arg(long, value_enum, default_value_t = SeedFmt::Text)]
    pub fmt: SeedFmt,
}

#[derive(Args)]
pub struct ApplyArgs {
    #[arg(long)]
    pub recipe: String,

    #[arg(long)]
    pub timemap: String,

    #[arg(long)]
    pub out: String,

    #[arg(long, value_enum, default_value_t = ApplyMode::Pair)]
    pub mode: ApplyMode,

    #[arg(long, default_value_t = 50_000_000)]
    pub max_ticks: u64,
}

#[derive(Args)]
pub struct FitArgs {
    #[arg(long)]
    pub recipe: String,

    #[arg(long)]
    pub target: String,

    #[arg(long)]
    pub out: String,

    #[arg(long, default_value_t = 2_000_000)]
    pub search_emissions: u64,

    #[arg(long, default_value_t = 80_000_000)]
    pub max_ticks: u64,

    #[arg(long, default_value_t = 0)]
    pub start_emission: u64,
}

#[derive(Args)]
pub struct FitXorArgs {
    #[arg(long)]
    pub recipe: String,

    #[arg(long)]
    pub target: String,

    #[arg(long)]
    pub out_timemap: String,

    #[arg(long)]
    pub out_residual: String,

    #[arg(long, value_enum, default_value_t = ApplyMode::Pair)]
    pub mode: ApplyMode,

    #[arg(long, value_enum, default_value_t = MapMode::None)]
    pub map: MapMode,

    #[arg(long, default_value_t = 0)]
    pub map_seed: u64,

    /// seed as hex (accepts "0x..." or raw hex). If set, overrides --map-seed.
    #[arg(long)]
    pub map_seed_hex: Option<String>,

    #[arg(long, value_enum, default_value_t = ResidualMode::Xor)]
    pub residual: ResidualMode,

    #[arg(long, default_value_t = 2_000_000)]
    pub search_emissions: u64,

    #[arg(long, default_value_t = 80_000_000)]
    pub max_ticks: u64,

    #[arg(long, default_value_t = 0)]
    pub start_emission: u64,

    #[arg(long, default_value_t = 1)]
    pub scan_step: usize,

    #[arg(long, value_enum, default_value_t = FitObjective::Zstd)]
    pub objective: FitObjective,

    #[arg(long, default_value_t = 3)]
    pub zstd_level: i32,
}

#[derive(Args, Clone)]
pub struct FitXorChunkedArgs {
    #[arg(long)]
    pub recipe: String,

    #[arg(long)]
    pub target: String,

    #[arg(long)]
    pub out_timemap: String,

    #[arg(long)]
    pub out_residual: String,

    #[arg(long, value_enum, default_value_t = ApplyMode::Pair)]
    pub mode: ApplyMode,

    #[arg(long, value_enum, default_value_t = MapMode::None)]
    pub map: MapMode,

    #[arg(long, default_value_t = 0)]
    pub map_seed: u64,

    /// seed as hex (accepts "0x..." or raw hex). If set, overrides --map-seed.
    #[arg(long)]
    pub map_seed_hex: Option<String>,

    #[arg(long, value_enum, default_value_t = ResidualMode::Xor)]
    pub residual: ResidualMode,

    #[arg(long, default_value_t = 2_000_000)]
    pub search_emissions: u64,

    #[arg(long, default_value_t = 80_000_000)]
    pub max_ticks: u64,

    #[arg(long, default_value_t = 0)]
    pub start_emission: u64,

    #[arg(long, default_value_t = 1)]
    pub scan_step: usize,

    #[arg(long, default_value_t = 3)]
    pub zstd_level: i32,

    #[arg(long, default_value_t = 512)]
    pub chunk_size: usize,

    #[arg(long, default_value_t = 0)]
    pub max_chunks: usize,

    #[arg(long, value_enum, default_value_t = FitObjective::Matches)]
    pub objective: FitObjective,

    #[arg(long, default_value_t = 256)]
    pub refine_topk: usize,

    /// cap how far forward (in stream BYTES) we will search from min_start for each chunk.
    /// prevents “teleporting” to the end of the stream.
    #[arg(long, default_value_t = 200_000)]
    pub lookahead: usize,

    // -------- NEW: bitfield params (used only when --map bitfield) --------

    /// For --map bitfield: number of bits per emission symbol (1..=8).
    #[arg(long, default_value_t = 2)]
    pub bits_per_emission: u8,

    /// For --map bitfield: mapping family (geom=steerable partitions, hash=baseline).
    #[arg(long, value_enum, default_value_t = BitMapping::Geom)]
    pub bit_mapping: BitMapping,
}

#[derive(Args)]
pub struct ReconstructArgs {
    #[arg(long)]
    pub recipe: String,

    #[arg(long)]
    pub timemap: String,

    #[arg(long)]
    pub residual: String,

    #[arg(long)]
    pub out: String,

    #[arg(long, value_enum, default_value_t = ApplyMode::Pair)]
    pub mode: ApplyMode,

    #[arg(long, value_enum, default_value_t = MapMode::None)]
    pub map: MapMode,

    #[arg(long, default_value_t = 0)]
    pub map_seed: u64,

    /// seed as hex (accepts "0x..." or raw hex). If set, overrides --map-seed.
    #[arg(long)]
    pub map_seed_hex: Option<String>,

    #[arg(long, value_enum, default_value_t = ResidualMode::Xor)]
    pub residual_mode: ResidualMode,

    #[arg(long, default_value_t = 80_000_000)]
    pub max_ticks: u64,

    // -------- NEW: bitfield params (used only when --map bitfield) --------
    #[arg(long, default_value_t = 2)]
    pub bits_per_emission: u8,

    #[arg(long, value_enum, default_value_t = BitMapping::Geom)]
    pub bit_mapping: BitMapping,
}

pub fn run(args: TimemapArgs) -> anyhow::Result<()> {
    match args.cmd {
        TimemapCmd::Make(a) => cmd_make(a),
        TimemapCmd::Inspect(a) => cmd_inspect(a),
        TimemapCmd::MapSeed(a) => cmd_map_seed(a),
        TimemapCmd::Apply(a) => cmd_apply(a),
        TimemapCmd::Fit(a) => cmd_fit(a),
        TimemapCmd::FitXor(a) => cmd_fit_xor(a),
        TimemapCmd::FitXorChunked(a) => cmd_fit_xor_chunked(a),
        TimemapCmd::Reconstruct(a) => cmd_reconstruct(a),
    }
}

fn cmd_make(a: MakeArgs) -> anyhow::Result<()> {
    let tm = TimingMap::stride(a.len, a.start, a.step).map_err(|e| anyhow::anyhow!("{e}"))?;
    timemap::write_tm1(&a.out, &tm)?;
    eprintln!(
        "timemap ok: out={} len={} start={} step={} last={:?}",
        a.out,
        tm.indices.len(),
        a.start,
        a.step,
        tm.last_index()
    );
    Ok(())
}

fn cmd_inspect(a: InspectArgs) -> anyhow::Result<()> {
    let tm = timemap::read_tm1(&a.r#in)?;
    eprintln!(
        "timemap: in={} len={} first={:?} last={:?}",
        a.r#in,
        tm.indices.len(),
        tm.indices.first(),
        tm.indices.last()
    );
    Ok(())
}

fn parse_seed(a: &MapSeedArgs) -> anyhow::Result<u64> {
    if let Some(s) = &a.map_seed_hex {
        let t = s.trim();
        let t = t.strip_prefix("0x").unwrap_or(t);
        let v = u64::from_str_radix(t, 16)
            .map_err(|e| anyhow::anyhow!("invalid --map-seed-hex ({s}): {e}"))?;
        Ok(v)
    } else {
        Ok(a.map_seed)
    }
}

/// Parse a seed given a decimal default and an optional hex override.
/// Accepts "0x..." or raw hex.
fn parse_seed_hex_opt(map_seed: u64, map_seed_hex: &Option<String>) -> anyhow::Result<u64> {
    if let Some(s) = map_seed_hex {
        let t = s.trim();
        let t = t.strip_prefix("0x").unwrap_or(t);
        let v = u64::from_str_radix(t, 16)
            .map_err(|e| anyhow::anyhow!("invalid --map-seed-hex ({s}): {e}"))?;
        Ok(v)
    } else {
        Ok(map_seed)
    }
}

fn cmd_map_seed(a: MapSeedArgs) -> anyhow::Result<()> {
    let seed = parse_seed(&a)?;
    let seed_hex = format!("0x{seed:016x}");

    match a.map {
        MapMode::Text40Field => {
            let seed_lo = seed as u32;
            let rate = ((seed >> 32) & 0xFF) as u8;
            let tshift = ((seed >> 40) & 0xFF) as u8;
            let phase0 = ((seed >> 48) & 0xFF) as u8;
            let shift_amp = ((seed >> 56) & 0xFF) as u8;

            match a.fmt {
                SeedFmt::Text => {
                    println!("map=text40-field");
                    println!("seed_dec={seed}");
                    println!("seed_hex={seed_hex}");
                    println!("seed_lo_u32={seed_lo}");
                    println!("rate_u8={rate}");
                    println!("tshift_u8={tshift}");
                    println!("phase0_u8={phase0}");
                    println!("shift_amp_u8={shift_amp}");
                }
                SeedFmt::Json => {
                    println!(
                        "{{\"map\":\"text40-field\",\"seed_dec\":{seed},\"seed_hex\":\"{seed_hex}\",\"seed_lo_u32\":{seed_lo},\"rate_u8\":{rate},\"tshift_u8\":{tshift},\"phase0_u8\":{phase0},\"shift_amp_u8\":{shift_amp}}}"
                    );
                }
            }
        }
        _ => match a.fmt {
            SeedFmt::Text => {
                println!("map={:?}", a.map);
                println!("seed_dec={seed}");
                println!("seed_hex={seed_hex}");
                println!("note=decoder-ring is only defined for text40-field currently");
            }
            SeedFmt::Json => {
                println!(
                    "{{\"map\":\"{:?}\",\"seed_dec\":{seed},\"seed_hex\":\"{seed_hex}\",\"note\":\"decoder-ring is only defined for text40-field currently\"}}",
                    a.map
                );
            }
        },
    }

    Ok(())
}

fn cmd_apply(a: ApplyArgs) -> anyhow::Result<()> {
    let recipe = recipe_file::load_k8r(&a.recipe)?;
    let tm = timemap::read_tm1(&a.timemap)?;

    if tm.indices.is_empty() {
        anyhow::bail!("timemap empty");
    }

    let mut engine = Engine::new(recipe)?;

    match a.mode {
        ApplyMode::Pair => {
            let bytes = collect_pair_bytes(&mut engine, &tm, a.max_ticks)?;
            std::fs::write(&a.out, &bytes)?;
            eprintln!(
                "apply ok: out={} bytes={} ticks={} emissions={}",
                a.out,
                bytes.len(),
                engine.stats.ticks,
                engine.stats.emissions
            );
        }
        ApplyMode::Rgbpair => {
            let bytes = collect_rgbpair_bytes(&mut engine, &tm, a.max_ticks)?;
            std::fs::write(&a.out, &bytes)?;
            eprintln!(
                "apply ok: out={} bytes={} ticks={} emissions={}",
                a.out,
                bytes.len(),
                engine.stats.ticks,
                engine.stats.emissions
            );
        }
    }

    Ok(())
}

fn cmd_fit(a: FitArgs) -> anyhow::Result<()> {
    let recipe = recipe_file::load_k8r(&a.recipe)?;
    let target = std::fs::read(&a.target)?;

    if target.is_empty() {
        anyhow::bail!("target is empty");
    }

    let mut engine = Engine::new(recipe)?;
    let mut indices: Vec<u64> = Vec::with_capacity(target.len());

    let mut want: usize = 0;
    let want_len = target.len();
    let first_byte = target[0];

    while (engine.stats.emissions as u64) < a.start_emission && engine.stats.ticks < a.max_ticks {
        let _ = engine.step();
        if (engine.stats.emissions as u64) >= a.search_emissions {
            break;
        }
    }

    let start_ticks = engine.stats.ticks;

    let mut first_byte_seen: u64 = 0;

    while (engine.stats.emissions as u64) < a.search_emissions && engine.stats.ticks < a.max_ticks {
        if let Some(tok) = engine.step() {
            let idx = (engine.stats.emissions - 1) as u64;
            let b = tok.pack_byte();

            if b == first_byte {
                first_byte_seen += 1;
            }

            if b == target[want] {
                indices.push(idx);
                want += 1;
                if want == want_len {
                    break;
                }
            }
        }
    }

    if want != want_len {
        anyhow::bail!(
            "timemap fit failed: matched {}/{} bytes; first_target=0x{:02x} first_seen={} start_emission={} searched_emissions={} ticks={} (start_ticks={} delta_ticks={})",
            want,
            want_len,
            first_byte,
            first_byte_seen,
            a.start_emission,
            engine.stats.emissions as u64,
            engine.stats.ticks,
            start_ticks,
            engine.stats.ticks.saturating_sub(start_ticks),
        );
    }

    let tm = TimingMap { indices };
    timemap::write_tm1(&a.out, &tm)?;

    eprintln!(
        "timemap fit ok: out={} target_bytes={} first_idx={:?} last_idx={:?} start_emission={} start_ticks={} end_emissions={} end_ticks={} delta_ticks={}",
        a.out,
        want_len,
        tm.indices.first(),
        tm.indices.last(),
        a.start_emission,
        start_ticks,
        engine.stats.emissions,
        engine.stats.ticks,
        engine.stats.ticks.saturating_sub(start_ticks),
    );

    Ok(())
}

fn cmd_fit_xor(a: FitXorArgs) -> anyhow::Result<()> {
    let recipe = recipe_file::load_k8r(&a.recipe)?;
    let recipe_raw_len = std::fs::read(&a.recipe).map(|b| b.len()).unwrap_or(0usize);

    let target = std::fs::read(&a.target)?;
    if target.is_empty() {
        anyhow::bail!("target is empty");
    }

    if a.scan_step == 0 {
        anyhow::bail!("--scan-step must be >= 1");
    }

    // allow hex override for map_seed
    let seed = parse_seed_hex_opt(a.map_seed, &a.map_seed_hex)?;

    let n = target.len();

    let bytes_per_emission: u64 = match a.mode {
        ApplyMode::Pair => 1,
        ApplyMode::Rgbpair => 6,
    };

    let mut engine = Engine::new(recipe)?;

    while (engine.stats.emissions as u64) < a.start_emission && engine.stats.ticks < a.max_ticks {
        let _ = engine.step();
        if (engine.stats.emissions as u64) >= a.search_emissions {
            break;
        }
    }

    let start_ticks = engine.stats.ticks;
    let start_em = engine.stats.emissions as u64;

    let mut stream: Vec<u8> = Vec::new();
    stream.reserve(
        ((a.search_emissions.saturating_sub(start_em)).min(200_000) * bytes_per_emission) as usize,
    );

    while (engine.stats.emissions as u64) < a.search_emissions && engine.stats.ticks < a.max_ticks {
        if let Some(tok) = engine.step() {
            match a.mode {
                ApplyMode::Pair => stream.push(tok.pack_byte()),
                ApplyMode::Rgbpair => stream.extend_from_slice(&tok.to_rgb_pair().to_bytes()),
            }
        }
    }

    if stream.len() < n {
        anyhow::bail!(
            "timemap fit-xor short: need at least {} stream bytes after start_emission={}, got {} (mode={:?}, ticks={} delta_ticks={})",
            n,
            a.start_emission,
            stream.len(),
            a.mode,
            engine.stats.ticks,
            engine.stats.ticks.saturating_sub(start_ticks),
        );
    }

    let max_start = stream.len() - n;
    let abs_stream_base_pos: u64 = a.start_emission * bytes_per_emission;

    let mut scratch_resid: Vec<u8> = vec![0u8; n];

    let mut best_start: usize = 0;
    let mut best_matches: u64 = 0;

    let mut best_zstd_resid: usize = usize::MAX;
    let mut best_score_effective: usize = usize::MAX;

    let mut scanned: u64 = 0;

    let mut s: usize = 0;
    while s <= max_start {
        scanned += 1;

        let base_pos = abs_stream_base_pos + (s as u64);
        let mut m: u64 = 0;

        for i in 0..n {
            let pos = base_pos + (i as u64);
            let mapped = map_byte(a.map, seed, pos, stream[s + i]);
            let resid = make_residual_byte(a.residual, mapped, target[i]);
            scratch_resid[i] = resid;
            if resid == 0 {
                m += 1;
            }
        }

        let zlen_resid = match a.objective {
            FitObjective::Matches => (n as u64).saturating_sub(m) as usize,
            FitObjective::Zstd => zstd_compress_len(&scratch_resid, a.zstd_level),
        };

        let tm1_raw_len = tm1_len_contig(base_pos, n);
        let score_effective = zlen_resid.saturating_add(tm1_raw_len);

        if score_effective < best_score_effective {
            best_score_effective = score_effective;
            best_zstd_resid = zlen_resid;
            best_start = s;
            best_matches = m;
            if best_score_effective == 0 {
                break;
            }
        }

        s = s.saturating_add(a.scan_step);
    }

    let abs_win_start_pos: u64 = abs_stream_base_pos + (best_start as u64);

    let indices: Vec<u64> = (0..(n as u64)).map(|i| abs_win_start_pos + i).collect();
    let tm = TimingMap { indices };

    let mut residual: Vec<u8> = Vec::with_capacity(n);
    for i in 0..n {
        let pos = abs_win_start_pos + (i as u64);
        let mapped = map_byte(a.map, seed, pos, stream[best_start + i]);
        residual.push(make_residual_byte(a.residual, mapped, target[i]));
    }

    let tm_bytes = tm.encode_tm1();
    let tm_raw = tm_bytes.len();
    let tm_zstd = zstd_compress_len(&tm_bytes, a.zstd_level);

    let resid_raw = residual.len();
    let resid_zstd = zstd_compress_len(&residual, a.zstd_level);

    let plain_zstd = zstd_compress_len(&target, a.zstd_level);

    let effective_no_recipe = tm_zstd.saturating_add(resid_zstd);
    let effective_with_recipe = recipe_raw_len.saturating_add(effective_no_recipe);

    timemap::write_tm1(&a.out_timemap, &tm)?;
    std::fs::write(&a.out_residual, &residual)?;

    eprintln!(
        "timemap fit-xor ok: mode={:?} map={:?} map_seed={} (0x{:016x}) residual={:?} objective={:?} scan_step={} scanned_windows={} zstd_level={} tm_out={} resid_out={} target_bytes={} matches={}/{} ({:.4}%) window_start_pos={} scanned_emissions={} stream_bytes={} ticks={}",
        a.mode,
        a.map,
        seed,
        seed,
        a.residual,
        a.objective,
        a.scan_step,
        scanned,
        a.zstd_level,
        a.out_timemap,
        a.out_residual,
        n,
        best_matches,
        n,
        (best_matches as f64) * 100.0 / (n as f64),
        abs_win_start_pos,
        (start_em + (stream.len() as u64 / bytes_per_emission)),
        stream.len(),
        engine.stats.ticks,
    );

    eprintln!("--- scoreboard ---");
    eprintln!("recipe_raw_bytes           = {}", recipe_raw_len);
    eprintln!("plain_raw_bytes            = {}", target.len());
    eprintln!("plain_zstd_bytes           = {}", plain_zstd);
    eprintln!("tm1_raw_bytes              = {}", tm_raw);
    eprintln!("tm1_zstd_bytes             = {}", tm_zstd);
    eprintln!("resid_raw_bytes            = {}", resid_raw);
    eprintln!("resid_zstd_bytes           = {}", resid_zstd);
    eprintln!("effective_bytes_no_recipe  = {}", effective_no_recipe);
    eprintln!("effective_bytes_with_recipe= {}", effective_with_recipe);
    eprintln!(
        "delta_vs_plain_zstd_no_recipe  = {}",
        (effective_no_recipe as i64) - (plain_zstd as i64)
    );
    eprintln!(
        "delta_vs_plain_zstd_with_recipe= {}",
        (effective_with_recipe as i64) - (plain_zstd as i64)
    );
    eprintln!("note_best_scan_score_proxy_or_zstd = {}", best_zstd_resid);
    eprintln!(
        "note_best_scan_effective_rawtm_plus_score = {}",
        best_score_effective
    );

    Ok(())
}

fn cmd_fit_xor_chunked(a: FitXorChunkedArgs) -> anyhow::Result<()> {
    // NEW: bitfield fork, keep the existing byte pipeline unchanged
    if a.map == MapMode::Bitfield {
        return cmd_fit_xor_chunked_bitfield(a);
    }

    let recipe = recipe_file::load_k8r(&a.recipe)?;
    let recipe_raw_len = std::fs::read(&a.recipe).map(|b| b.len()).unwrap_or(0usize);

    let target = std::fs::read(&a.target)?;
    if target.is_empty() {
        anyhow::bail!("target is empty");
    }
    if a.chunk_size == 0 {
        anyhow::bail!("--chunk-size must be >= 1");
    }
    if a.scan_step == 0 {
        anyhow::bail!("--scan-step must be >= 1");
    }

    // allow hex override for map_seed
    let seed = parse_seed_hex_opt(a.map_seed, &a.map_seed_hex)?;

    let bytes_per_emission: u64 = match a.mode {
        ApplyMode::Pair => 1,
        ApplyMode::Rgbpair => 6,
    };

    let mut engine = Engine::new(recipe)?;

    while (engine.stats.emissions as u64) < a.start_emission && engine.stats.ticks < a.max_ticks {
        let _ = engine.step();
        if (engine.stats.emissions as u64) >= a.search_emissions {
            break;
        }
    }

    let start_ticks = engine.stats.ticks;
    let start_em = engine.stats.emissions as u64;

    let mut stream: Vec<u8> = Vec::new();
    stream.reserve(
        ((a.search_emissions.saturating_sub(start_em)).min(500_000) * bytes_per_emission) as usize,
    );

    while (engine.stats.emissions as u64) < a.search_emissions && engine.stats.ticks < a.max_ticks {
        if let Some(tok) = engine.step() {
            match a.mode {
                ApplyMode::Pair => stream.push(tok.pack_byte()),
                ApplyMode::Rgbpair => stream.extend_from_slice(&tok.to_rgb_pair().to_bytes()),
            }
        }
    }

    let abs_stream_base_pos: u64 = a.start_emission * bytes_per_emission;
    let total_n = target.len();

    let mut tm_indices: Vec<u64> = Vec::with_capacity(total_n);
    let mut residual: Vec<u8> = Vec::with_capacity(total_n);

    eprintln!(
        "--- fit-xor-chunked --- mode={:?} map={:?} map_seed={} (0x{:016x}) residual={:?} objective={:?} refine_topk={} lookahead={} chunk_size={} scan_step={} zstd_level={} target_bytes={} stream_bytes={} base_pos={} start_emission={} end_emissions={} ticks={} delta_ticks={}",
        a.mode,
        a.map,
        seed,
        seed,
        a.residual,
        a.objective,
        a.refine_topk,
        a.lookahead,
        a.chunk_size,
        a.scan_step,
        a.zstd_level,
        total_n,
        stream.len(),
        abs_stream_base_pos,
        a.start_emission,
        (start_em + (stream.len() as u64 / bytes_per_emission)),
        engine.stats.ticks,
        engine.stats.ticks.saturating_sub(start_ticks),
    );

    let mut prev_pos: Option<u64> = None;
    let mut chunk_idx: usize = 0;

    let mut off: usize = 0;
    while off < total_n {
        if a.max_chunks != 0 && chunk_idx >= a.max_chunks {
            break;
        }

        let remaining_total = total_n - off;
        let n = remaining_total.min(a.chunk_size);

        let min_pos: u64 = match prev_pos {
            None => abs_stream_base_pos,
            Some(p) => p.saturating_add(1),
        };

        let min_start: usize = (min_pos - abs_stream_base_pos) as usize;

        let max_start_cap = min_start.saturating_add(a.lookahead);

        let need_min = min_start.saturating_add(n);
        if need_min > stream.len() {
            let ok = ensure_stream_len(
                &mut engine,
                &mut stream,
                need_min,
                a.mode,
                a.search_emissions,
                a.max_ticks,
            );
            if !ok {
                eprintln!(
                    "no room for chunk {}: need_len={} min_pos={} min_start={} stream_len={} chunk_len={} (writing partial)",
                    chunk_idx,
                    need_min,
                    min_pos,
                    min_start,
                    stream.len(),
                    n
                );
                break;
            }
        }

        let need_finish_from_min = min_start.saturating_add(remaining_total);
        if need_finish_from_min > stream.len() {
            let ok = ensure_stream_len(
                &mut engine,
                &mut stream,
                need_finish_from_min,
                a.mode,
                a.search_emissions,
                a.max_ticks,
            );
            if !ok {
                eprintln!(
                    "no room to finish from min_start for chunk {}: need_finish={} min_start={} stream_len={} remaining_total={} (writing partial)",
                    chunk_idx,
                    need_finish_from_min,
                    min_start,
                    stream.len(),
                    remaining_total
                );
                break;
            }
        }

        let max_start_possible = if stream.len() >= n { stream.len() - n } else { 0 };
        let max_start_finish = stream.len().saturating_sub(remaining_total);
        let max_start: usize = max_start_possible.min(max_start_cap).min(max_start_finish);

        if min_start > max_start {
            eprintln!(
                "no legal window for chunk {}: min_start={} max_start={} stream_len={} chunk_len={} remaining_total={} (writing partial)",
                chunk_idx,
                min_start,
                max_start,
                stream.len(),
                n,
                remaining_total
            );
            break;
        }

        let mut scratch_resid: Vec<u8> = vec![0u8; n];

        let mut best_start_proxy: usize = min_start;
        let mut best_matches_proxy: u64 = 0;
        let mut best_proxy_score: usize = usize::MAX;

        let mut refine: Vec<(usize, usize, u64)> = Vec::new();

        let mut scanned: u64 = 0;

        let mut s: usize = min_start;
        while s <= max_start {
            scanned += 1;

            let base_pos = abs_stream_base_pos + (s as u64);

            let mut matches: u64 = 0;
            for i in 0..n {
                let pos = base_pos + (i as u64);
                let mapped = map_byte(a.map, seed, pos, stream[s + i]);
                let resid_b = make_residual_byte(a.residual, mapped, target[off + i]);
                scratch_resid[i] = resid_b;
                if resid_b == 0 {
                    matches += 1;
                }
            }

            let jump_cost = tm_jump_cost(prev_pos, base_pos);

            let proxy_cost = (n as u64).saturating_sub(matches) as usize;
            let proxy_score = proxy_cost.saturating_add(jump_cost);

            if proxy_score < best_proxy_score
                || (proxy_score == best_proxy_score && s < best_start_proxy)
            {
                best_proxy_score = proxy_score;
                best_start_proxy = s;
                best_matches_proxy = matches;
            }

            if a.objective == FitObjective::Matches && a.refine_topk != 0 {
                refine.push((proxy_score, s, matches));
            }

            if a.objective == FitObjective::Zstd {
                let zlen = zstd_compress_len(&scratch_resid, a.zstd_level);
                let score = zlen.saturating_add(jump_cost);

                if score < best_proxy_score || (score == best_proxy_score && s < best_start_proxy) {
                    best_proxy_score = score;
                    best_start_proxy = s;
                    best_matches_proxy = matches;
                }
            }

            s = s.saturating_add(a.scan_step);
        }

        let mut best_start: usize = best_start_proxy;
        let mut best_matches: u64 = best_matches_proxy;
        let mut best_score: usize = best_proxy_score;
        let mut best_resid_zstd: usize = usize::MAX;

        if a.objective == FitObjective::Matches && a.refine_topk != 0 && !refine.is_empty() {
            refine.sort_by(|a1, b1| a1.0.cmp(&b1.0).then_with(|| a1.1.cmp(&b1.1)));
            if refine.len() > a.refine_topk {
                refine.truncate(a.refine_topk);
            }

            for &(_proxy_score, cand_s, cand_matches) in refine.iter() {
                let base_pos = abs_stream_base_pos + (cand_s as u64);
                let jump_cost = tm_jump_cost(prev_pos, base_pos);

                for i in 0..n {
                    let pos = base_pos + (i as u64);
                    let mapped = map_byte(a.map, seed, pos, stream[cand_s + i]);
                    scratch_resid[i] = make_residual_byte(a.residual, mapped, target[off + i]);
                }

                let zlen = zstd_compress_len(&scratch_resid, a.zstd_level);
                let score = zlen.saturating_add(jump_cost);

                if score < best_score || (score == best_score && cand_s < best_start) {
                    best_score = score;
                    best_start = cand_s;
                    best_matches = cand_matches;
                    best_resid_zstd = zlen;
                }
            }

            if best_resid_zstd == usize::MAX {
                let base_pos = abs_stream_base_pos + (best_start as u64);
                let jump_cost = tm_jump_cost(prev_pos, base_pos);
                for i in 0..n {
                    let pos = base_pos + (i as u64);
                    let mapped = map_byte(a.map, seed, pos, stream[best_start + i]);
                    scratch_resid[i] = make_residual_byte(a.residual, mapped, target[off + i]);
                }
                let zlen = zstd_compress_len(&scratch_resid, a.zstd_level);
                best_resid_zstd = zlen;
                best_score = zlen.saturating_add(jump_cost);
            }
        }

        let base_pos = abs_stream_base_pos + (best_start as u64);
        let jump_cost = tm_jump_cost(prev_pos, base_pos);

        for i in 0..n {
            let pos = base_pos + (i as u64);
            let mapped = map_byte(a.map, seed, pos, stream[best_start + i]);
            tm_indices.push(pos);
            residual.push(make_residual_byte(a.residual, mapped, target[off + i]));
        }

        prev_pos = Some(base_pos + (n as u64) - 1);

        let (emission, lane) = match a.mode {
            ApplyMode::Pair => (base_pos, 0),
            ApplyMode::Rgbpair => (base_pos / 6, (base_pos % 6) as u64),
        };

        let printed_resid_zstd = if a.objective == FitObjective::Zstd {
            let mut scratch: Vec<u8> = vec![0u8; n];
            for i in 0..n {
                let pos = base_pos + (i as u64);
                let mapped = map_byte(a.map, seed, pos, stream[best_start + i]);
                scratch[i] = make_residual_byte(a.residual, mapped, target[off + i]);
            }
            zstd_compress_len(&scratch, a.zstd_level)
        } else if best_resid_zstd != usize::MAX {
            best_resid_zstd
        } else {
            (n as u64).saturating_sub(best_matches) as usize
        };

        eprintln!(
            "chunk {:04} off={} len={} start_pos={} (emission={} lane={}) scanned_windows={} matches={}/{} ({:.2}%) jump_cost={} chunk_score={} chunk_resid_metric={}",
            chunk_idx,
            off,
            n,
            base_pos,
            emission,
            lane,
            scanned,
            best_matches,
            n,
            (best_matches as f64) * 100.0 / (n as f64),
            jump_cost,
            best_score,
            printed_resid_zstd
        );

        off += n;
        chunk_idx += 1;
    }

    if tm_indices.len() != residual.len() {
        anyhow::bail!(
            "internal: tm_indices/residual len mismatch: tm={} resid={}",
            tm_indices.len(),
            residual.len()
        );
    }
    if tm_indices.is_empty() {
        anyhow::bail!("no output produced");
    }

    let produced = residual.len();
    if produced != target.len() {
        eprintln!(
            "note: partial output produced_bytes={} target_bytes={}",
            produced,
            target.len()
        );
    }

    let tm = TimingMap { indices: tm_indices };

    let tm_bytes = tm.encode_tm1();
    let tm_raw = tm_bytes.len();
    let tm_zstd = zstd_compress_len(&tm_bytes, a.zstd_level);

    let resid_raw = residual.len();
    let resid_zstd = zstd_compress_len(&residual, a.zstd_level);

    let target_slice = &target[..produced];
    let plain_zstd = zstd_compress_len(target_slice, a.zstd_level);

    let effective_no_recipe = tm_zstd.saturating_add(resid_zstd);
    let effective_with_recipe = recipe_raw_len.saturating_add(effective_no_recipe);

    timemap::write_tm1(&a.out_timemap, &tm)?;
    std::fs::write(&a.out_residual, &residual)?;

    eprintln!("--- scoreboard ---");
    eprintln!("recipe_raw_bytes           = {}", recipe_raw_len);
    eprintln!("plain_raw_bytes            = {}", produced);
    eprintln!("plain_zstd_bytes           = {}", plain_zstd);
    eprintln!("tm1_raw_bytes              = {}", tm_raw);
    eprintln!("tm1_zstd_bytes             = {}", tm_zstd);
    eprintln!("resid_raw_bytes            = {}", resid_raw);
    eprintln!("resid_zstd_bytes           = {}", resid_zstd);
    eprintln!("effective_bytes_no_recipe  = {}", effective_no_recipe);
    eprintln!("effective_bytes_with_recipe= {}", effective_with_recipe);
    eprintln!(
        "delta_vs_plain_zstd_no_recipe  = {}",
        (effective_no_recipe as i64) - (plain_zstd as i64)
    );
    eprintln!(
        "delta_vs_plain_zstd_with_recipe= {}",
        (effective_with_recipe as i64) - (plain_zstd as i64)
    );

    Ok(())
}

fn ensure_stream_len(
    engine: &mut Engine,
    stream: &mut Vec<u8>,
    need_len: usize,
    mode: ApplyMode,
    search_emissions: u64,
    max_ticks: u64,
) -> bool {
    if stream.len() >= need_len {
        return true;
    }

    while stream.len() < need_len
        && (engine.stats.emissions as u64) < search_emissions
        && engine.stats.ticks < max_ticks
    {
        if let Some(tok) = engine.step() {
            match mode {
                ApplyMode::Pair => stream.push(tok.pack_byte()),
                ApplyMode::Rgbpair => stream.extend_from_slice(&tok.to_rgb_pair().to_bytes()),
            }
        }
    }

    stream.len() >= need_len
}

fn cmd_reconstruct(a: ReconstructArgs) -> anyhow::Result<()> {
    let recipe = recipe_file::load_k8r(&a.recipe)?;
    let tm = timemap::read_tm1(&a.timemap)?;

    if tm.indices.is_empty() {
        anyhow::bail!("timemap empty");
    }

    // allow hex override for map_seed
    let seed = parse_seed_hex_opt(a.map_seed, &a.map_seed_hex)?;

    // NEW: bitfield reconstruct path
    if a.map == MapMode::Bitfield {
        if a.mode != ApplyMode::Rgbpair {
            anyhow::bail!("--map bitfield requires --mode rgbpair");
        }
        if a.bits_per_emission == 0 || a.bits_per_emission > 8 {
            anyhow::bail!("--bits-per-emission must be in 1..=8");
        }

        let bf = read_bitfield_residual(&a.residual)?;
        if bf.bits_per_emission != a.bits_per_emission {
            anyhow::bail!(
                "bitfield residual bits_per_emission mismatch: file={} cli={}",
                bf.bits_per_emission,
                a.bits_per_emission
            );
        }
        if bf.mapping != a.bit_mapping {
            anyhow::bail!(
                "bitfield residual mapping mismatch: file={:?} cli={:?}",
                bf.mapping,
                a.bit_mapping
            );
        }

        if tm.indices.len() != bf.symbol_count {
            anyhow::bail!(
                "timemap/residual symbol_count mismatch: tm={} resid_symbols={}",
                tm.indices.len(),
                bf.symbol_count
            );
        }

        let resid_syms =
            bitpack::unpack_symbols(bf.bits_per_emission, &bf.packed_symbols, bf.symbol_count)
                .map_err(|e| anyhow::anyhow!("{e}"))?;

        let mut engine = Engine::new(recipe)?;

        let mut max_idx: u64 = 0;
        for &idx in tm.indices.iter() {
            if idx > max_idx {
                max_idx = idx;
            }
        }

        let mut out_syms: Vec<u8> = Vec::with_capacity(bf.symbol_count);
        let mut i: usize = 0;

        let mask = sym_mask(a.bits_per_emission);

        while engine.stats.ticks < a.max_ticks && (engine.stats.emissions as u64) <= max_idx {
            if let Some(tok) = engine.step() {
                let em = (engine.stats.emissions - 1) as u64;

                while i < tm.indices.len() && tm.indices[i] == em {
                    let rgb6 = tok.to_rgb_pair().to_bytes();
                    let pred =
                        map_symbol_bitfield(a.bit_mapping, seed, em, &rgb6, a.bits_per_emission)
                            & mask;
                    let sym =
                        apply_residual_symbol(a.residual_mode, pred, resid_syms[i] & mask, mask);
                    out_syms.push(sym);
                    i += 1;
                }
            }
        }

        if i != tm.indices.len() {
            anyhow::bail!(
                "reconstruct short (bitfield): wrote {} of {} symbols (max_idx={} ticks={} emissions={})",
                i,
                tm.indices.len(),
                max_idx,
                engine.stats.ticks,
                engine.stats.emissions
            );
        }

        let mut out_bytes =
            bitpack::pack_symbols(a.bits_per_emission, &out_syms).map_err(|e| anyhow::anyhow!("{e}"))?;
        out_bytes.truncate(bf.orig_len_bytes);

        std::fs::write(&a.out, &out_bytes)?;
        eprintln!(
            "reconstruct ok (bitfield): out={} bytes={} symbols={} bits_per_emission={} bit_mapping={:?} ticks={} emissions={} map_seed={} (0x{:016x})",
            a.out,
            out_bytes.len(),
            out_syms.len(),
            a.bits_per_emission,
            a.bit_mapping,
            engine.stats.ticks,
            engine.stats.emissions,
            seed,
            seed
        );
        return Ok(());
    }

    // normal byte residual path (unchanged)
    let resid = std::fs::read(&a.residual)?;

    if tm.indices.len() != resid.len() {
        anyhow::bail!(
            "timemap/residual len mismatch: tm={} resid={}",
            tm.indices.len(),
            resid.len()
        );
    }

    let mut engine = Engine::new(recipe)?;

    let mut out: Vec<u8> = Vec::with_capacity(resid.len());

    let mut i: usize = 0;

    let mut max_idx: u64 = 0;
    for &idx in tm.indices.iter() {
        if idx > max_idx {
            max_idx = idx;
        }
    }

    match a.mode {
        ApplyMode::Pair => {
            while engine.stats.ticks < a.max_ticks && (engine.stats.emissions as u64) <= max_idx {
                if let Some(tok) = engine.step() {
                    let idx = (engine.stats.emissions - 1) as u64;

                    while i < tm.indices.len() && tm.indices[i] == idx {
                        let mapped = map_byte(a.map, seed, idx, tok.pack_byte());
                        out.push(apply_residual_byte(a.residual_mode, mapped, resid[i]));
                        i += 1;
                    }
                }
            }
        }
        ApplyMode::Rgbpair => {
            while engine.stats.ticks < a.max_ticks && ((engine.stats.emissions as u64) * 6) <= max_idx
            {
                if let Some(tok) = engine.step() {
                    let em = (engine.stats.emissions - 1) as u64;
                    let base = em * 6;
                    let rgb6 = tok.to_rgb_pair().to_bytes();

                    for lane in 0..6u64 {
                        let pos = base + lane;
                        if pos > max_idx {
                            break;
                        }
                        while i < tm.indices.len() && tm.indices[i] == pos {
                            let mapped = map_byte(a.map, seed, pos, rgb6[lane as usize]);
                            out.push(apply_residual_byte(a.residual_mode, mapped, resid[i]));
                            i += 1;
                        }
                    }
                }
            }
        }
    }

    if i != tm.indices.len() {
        anyhow::bail!(
            "reconstruct short: wrote {} of {} bytes (max_idx={} ticks={} emissions={})",
            i,
            tm.indices.len(),
            max_idx,
            engine.stats.ticks,
            engine.stats.emissions
        );
    }

    std::fs::write(&a.out, &out)?;
    eprintln!(
        "reconstruct ok: out={} bytes={} ticks={} emissions={} map_seed={} (0x{:016x})",
        a.out,
        out.len(),
        engine.stats.ticks,
        engine.stats.emissions,
        seed,
        seed
    );
    Ok(())
}

fn collect_pair_bytes(engine: &mut Engine, tm: &TimingMap, max_ticks: u64) -> anyhow::Result<Vec<u8>> {
    let mut out: Vec<u8> = Vec::with_capacity(tm.indices.len());

    let mut i: usize = 0;
    let max_idx = *tm.indices.last().unwrap_or(&0);

    while engine.stats.ticks < max_ticks && (engine.stats.emissions as u64) <= max_idx {
        if let Some(tok) = engine.step() {
            let idx = (engine.stats.emissions - 1) as u64;

            while i < tm.indices.len() && tm.indices[i] == idx {
                out.push(tok.pack_byte());
                i += 1;
            }
        }
    }

    if i != tm.indices.len() {
        anyhow::bail!(
            "apply short: wrote {} of {} bytes (max_idx={} ticks={} emissions={})",
            i,
            tm.indices.len(),
            max_idx,
            engine.stats.ticks,
            engine.stats.emissions
        );
    }

    Ok(out)
}

fn collect_rgbpair_bytes(
    engine: &mut Engine,
    tm: &TimingMap,
    max_ticks: u64,
) -> anyhow::Result<Vec<u8>> {
    let mut out: Vec<u8> = Vec::with_capacity(tm.indices.len());

    let mut i: usize = 0;
    let max_idx = *tm.indices.last().unwrap_or(&0);

    while engine.stats.ticks < max_ticks && ((engine.stats.emissions as u64) * 6) <= max_idx {
        if let Some(tok) = engine.step() {
            let em = (engine.stats.emissions - 1) as u64;
            let base = em * 6;
            let rgb6 = tok.to_rgb_pair().to_bytes();

            for lane in 0..6u64 {
                let pos = base + lane;
                if pos > max_idx {
                    break;
                }
                while i < tm.indices.len() && tm.indices[i] == pos {
                    out.push(rgb6[lane as usize]);
                    i += 1;
                }
            }
        }
    }

    if i != tm.indices.len() {
        anyhow::bail!(
            "apply short: wrote {} of {} bytes (max_idx={} ticks={} emissions={})",
            i,
            tm.indices.len(),
            max_idx,
            engine.stats.ticks,
            engine.stats.emissions
        );
    }

    Ok(out)
}

fn map_byte(mode: MapMode, seed: u64, pos: u64, raw: u8) -> u8 {
    match mode {
        MapMode::None => raw,
        MapMode::Splitmix64 => {
            let k = splitmix64(seed ^ pos) as u8;
            raw ^ k
        }
        MapMode::Ascii7 => ascii7(raw),
        MapMode::Ascii7Splitmix => {
            let k = splitmix64(seed ^ pos) as u8;
            ascii7(raw ^ k)
        }
        MapMode::Text40 => text_from_alphabet(TEXT40_ALPHABET, raw),
        MapMode::Text40Weighted => text_from_weighted_alphabet(TEXT40_ALPHABET, TEXT40_WEIGHTS, raw),
        MapMode::Text40Lane => text40_lane(pos, raw),
        MapMode::Text40Field => text40_field(seed, pos, raw),
        MapMode::Bitfield => raw, // not used in byte pipeline
        MapMode::Text64 => text_from_alphabet(TEXT64_ALPHABET, raw),
    }
}

fn ascii7(b: u8) -> u8 {
    let x = b & 0x7F;
    if (0x20..=0x7E).contains(&x) {
        x
    } else {
        0x20u8 + (x % 95)
    }
}

fn text_from_alphabet(alpha: &[u8], raw: u8) -> u8 {
    let idx = (raw as usize) % alpha.len();
    alpha[idx]
}

fn text_from_weighted_alphabet(alpha: &[u8], weights: &[u8], raw: u8) -> u8 {
    debug_assert_eq!(alpha.len(), weights.len());
    let mut x: u16 = raw as u16;

    for (i, &w) in weights.iter().enumerate() {
        let ww = w as u16;
        if x < ww {
            return alpha[i];
        }
        x -= ww;
    }

    let idx = (raw as usize) % alpha.len();
    alpha[idx]
}

fn text40_lane(pos: u64, raw: u8) -> u8 {
    let lane = (pos % 6) as u8;

    match lane {
        0 => text_from_weighted_alphabet(LANE0_ALPHA, LANE0_W, raw),
        1 => text_from_weighted_alphabet(LANE1_ALPHA, LANE1_W, raw),
        2 => text_from_weighted_alphabet(LANE2_ALPHA, LANE2_W, raw),
        3 => text_from_weighted_alphabet(LANE3_ALPHA, LANE3_W, raw),
        4 => text_from_weighted_alphabet(LANE4_ALPHA, LANE4_W, raw),
        _ => text_from_weighted_alphabet(LANE5_ALPHA, LANE5_W, raw),
    }
}

/// Text40Field mapping with TIME-EVOLVING “intensity” (vision-aligned) + NEW “shift-wave”.
///
/// `seed` packs extra params without changing CLI:
/// - seed_lo   (lower 32 bits): base noise seed
/// - rate      ((seed>>32) & 0xFF): intensity strength (0..255; 0 treated as 1)
/// - tshift    ((seed>>40) & 0xFF): time scale; pos >> tshift drives evolution
/// - phase0    ((seed>>48) & 0xFF): phase offset for the cycle
/// - shift_amp ((seed>>56) & 0xFF): 0 disables; otherwise adds a wave-like SHIFT to stripe/phase
fn text40_field(seed: u64, pos: u64, raw: u8) -> u8 {
    let lane = (pos % 6) as u8;

    let mut stripe = ((pos >> 7) & 0xFF) as u8;
    let mut phase = ((pos >> 11) & 0xFF) as u8;

    let seed_lo = seed as u32;
    let rate = ((seed >> 32) & 0xFF) as u8;
    let tshift = ((seed >> 40) & 0xFF) as u8;
    let phase0 = ((seed >> 48) & 0xFF) as u8;
    let shift_amp = ((seed >> 56) & 0xFF) as u8;

    let sh = (tshift as u32).min(56);
    let t = ((pos >> sh) & 0xFFFF) as u16;
    let t8 = (t as u8).wrapping_add(phase0);

    let tri = {
        let x = t8 & 0x7F;
        if (t8 & 0x80) == 0 {
            x.wrapping_mul(2)
        } else {
            (127u8 - x).wrapping_mul(2)
        }
    };

    if shift_amp != 0 {
        let w = tri
            .wrapping_add(lane.wrapping_mul(31))
            .wrapping_add(phase0);
        let centered = (w as i16) - 128i16;
        let scaled = (centered * (shift_amp as i16)) / 256i16;
        stripe = stripe.wrapping_add(scaled as i8 as u8);
        phase = phase.wrapping_add(((scaled / 2) as i8) as u8);
    }

    let noise = (splitmix64((seed_lo as u64) ^ pos) as u8).wrapping_mul(13);

    let r = if rate == 0 { 1u8 } else { rate };
    let f = stripe
        .wrapping_add(phase)
        .wrapping_add(lane.wrapping_mul(17))
        .wrapping_add(noise)
        .wrapping_add(tri.wrapping_mul(r));

    let mixed = raw.wrapping_add(f);

    match lane {
        0 => text_from_weighted_alphabet(LANE0_ALPHA, LANE0_W, mixed),
        1 => text_from_weighted_alphabet(LANE1_ALPHA, LANE1_W, mixed),
        2 => text_from_weighted_alphabet(LANE2_ALPHA, LANE2_W, mixed),
        3 => text_from_weighted_alphabet(LANE3_ALPHA, LANE3_W, mixed),
        4 => text_from_weighted_alphabet(LANE4_ALPHA, LANE4_W, mixed),
        _ => text_from_weighted_alphabet(LANE5_ALPHA, LANE5_W, mixed),
    }
}

const TEXT40_ALPHABET: &[u8] = b" etaoinshrdlucmfwypvbgkjqxz\n.,'";
const TEXT40_WEIGHTS: &[u8] = &[
    58, 22, 16, 16, 14, 13, 12, 10, 9, 9, 8, 8, 6, 6, 6, 5, 4, 4, 4, 3, 3, 3, 2, 1, 1, 1, 1, 6, 2, 2,
    1,
];

const LANE0_ALPHA: &[u8] = b" \n.,'";
const LANE0_W: &[u8] = &[200, 40, 6, 6, 4];

const LANE1_ALPHA: &[u8] = b" aeiou";
const LANE1_W: &[u8] = &[64, 48, 56, 44, 28, 16];

const LANE2_ALPHA: &[u8] = b" nstrhl";
const LANE2_W: &[u8] = &[64, 44, 44, 42, 36, 32, 38];

const LANE3_ALPHA: &[u8] = b" dcmfwypvbgkjqxz";
const LANE3_W: &[u8] = &[64, 20, 20, 18, 18, 16, 16, 14, 12, 12, 12, 8, 4, 4, 4, 4];

const LANE4_ALPHA: &[u8] = b" \n.,'";
const LANE4_W: &[u8] = &[140, 44, 28, 28, 16];

const LANE5_ALPHA: &[u8] = b" etaoinshrdl";
const LANE5_W: &[u8] = &[96, 18, 14, 14, 12, 12, 10, 10, 10, 8, 6, 6];

const TEXT64_ALPHABET: &[u8] =
    b" etaoinshrdlucmfwypvbgkjqxzETAOINSHRDLUCMFWYPVBGKJQXZ\n.,;:'\"-?!0123456789";

fn make_residual_byte(mode: ResidualMode, model: u8, plain: u8) -> u8 {
    match mode {
        ResidualMode::Xor => model ^ plain,
        ResidualMode::Sub => plain.wrapping_sub(model),
    }
}

fn apply_residual_byte(mode: ResidualMode, model: u8, resid: u8) -> u8 {
    match mode {
        ResidualMode::Xor => model ^ resid,
        ResidualMode::Sub => model.wrapping_add(resid),
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

fn zstd_compress_len(bytes: &[u8], level: i32) -> usize {
    zstd::encode_all(bytes, level)
        .map(|v| v.len())
        .unwrap_or(usize::MAX)
}

fn var_u64_len(mut x: u64) -> usize {
    let mut n: usize = 1;
    while x >= 0x80 {
        n += 1;
        x >>= 7;
    }
    n
}

fn tm1_len_contig(start_pos: u64, n: usize) -> usize {
    let magic = 4usize;
    let count = var_u64_len(n as u64);
    let delta0 = var_u64_len(start_pos);
    let deltas_rest = if n <= 1 { 0usize } else { n - 1 };
    magic + count + delta0 + deltas_rest
}

fn tm_jump_cost(prev_pos: Option<u64>, next_start_pos: u64) -> usize {
    match prev_pos {
        None => var_u64_len(next_start_pos),
        Some(p) => {
            let delta = next_start_pos.saturating_sub(p);
            var_u64_len(delta)
        }
    }
}

// ==========================================================
// NEW: Bitfield/Symbolfield fit-xor-chunked + residual format
// ==========================================================

const BF_MAGIC: &[u8; 4] = b"BF1\0";

#[derive(Clone, Debug)]
struct BitfieldResidual {
    bits_per_emission: u8,
    mapping: BitMapping,
    orig_len_bytes: usize,
    symbol_count: usize,
    packed_symbols: Vec<u8>,
}

fn sym_mask(bits_per_emission: u8) -> u8 {
    if bits_per_emission == 0 {
        0
    } else if bits_per_emission >= 8 {
        0xFF
    } else {
        ((1u16 << bits_per_emission) - 1) as u8
    }
}

fn make_residual_symbol(mode: ResidualMode, model: u8, plain: u8, mask: u8) -> u8 {
    match mode {
        ResidualMode::Xor => (model ^ plain) & mask,
        ResidualMode::Sub => plain.wrapping_sub(model) & mask,
    }
}

fn apply_residual_symbol(mode: ResidualMode, model: u8, resid: u8, mask: u8) -> u8 {
    match mode {
        ResidualMode::Xor => (model ^ resid) & mask,
        ResidualMode::Sub => model.wrapping_add(resid) & mask,
    }
}

fn read_bitfield_residual(path: &str) -> anyhow::Result<BitfieldResidual> {
    let bytes = std::fs::read(path)?;
    if bytes.len() < 24 {
        anyhow::bail!("bitfield residual too small: {} bytes", bytes.len());
    }
    if &bytes[0..4] != BF_MAGIC {
        anyhow::bail!("bitfield residual bad magic");
    }

    let bits = bytes[4];
    let mapping_u8 = bytes[5];
    let mapping = match mapping_u8 {
        0 => BitMapping::Geom,
        1 => BitMapping::Hash,
        _ => anyhow::bail!("bitfield residual unknown mapping tag: {}", mapping_u8),
    };

    let orig_len_bytes = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
    let symbol_count = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;

    let payload = bytes[24..].to_vec();

    Ok(BitfieldResidual {
        bits_per_emission: bits,
        mapping,
        orig_len_bytes,
        symbol_count,
        packed_symbols: payload,
    })
}

fn write_bitfield_residual(
    path: &str,
    bits_per_emission: u8,
    mapping: BitMapping,
    orig_len_bytes: usize,
    residual_symbols: &[u8],
) -> anyhow::Result<Vec<u8>> {
    let packed = bitpack::pack_symbols(bits_per_emission, residual_symbols)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut out: Vec<u8> = Vec::with_capacity(24 + packed.len());
    out.extend_from_slice(BF_MAGIC);
    out.push(bits_per_emission);
    out.push(match mapping {
        BitMapping::Geom => 0u8,
        BitMapping::Hash => 1u8,
    });
    out.extend_from_slice(&[0u8, 0u8]); // padding
    out.extend_from_slice(&(orig_len_bytes as u64).to_le_bytes());
    out.extend_from_slice(&(residual_symbols.len() as u64).to_le_bytes());
    out.extend_from_slice(&packed);

    std::fs::write(path, &out)?;
    Ok(packed)
}

fn map_symbol_bitfield(
    mapping: BitMapping,
    map_seed: u64,
    emission: u64,
    rgb6: &[u8; 6],
    bits_per_emission: u8,
) -> u8 {
    let mask = sym_mask(bits_per_emission);

    match mapping {
        BitMapping::Geom => {
            // "vision-aligned": use simple, steerable geometric comparisons
            // derived from the two RGB triplets (A and C "dots" meeting at B).
            let r = ((rgb6[0] as u16) + (rgb6[3] as u16)) / 2;
            let g = ((rgb6[1] as u16) + (rgb6[4] as u16)) / 2;
            let b = ((rgb6[2] as u16) + (rgb6[5] as u16)) / 2;

            let mut sym: u8 = 0;

            if bits_per_emission >= 1 {
                sym |= ((r > g) as u8) << 0;
            }
            if bits_per_emission >= 2 {
                sym |= ((b > g) as u8) << 1;
            }
            if bits_per_emission >= 3 {
                sym |= ((r > b) as u8) << 2;
            }
            if bits_per_emission >= 4 {
                sym |= ((g > r) as u8) << 3;
            }
            if bits_per_emission >= 5 {
                sym |= ((g > b) as u8) << 4;
            }
            if bits_per_emission >= 6 {
                let y = r + g + b;
                sym |= ((y > (3 * 128)) as u8) << 5;
            }
            if bits_per_emission >= 7 {
                sym |= (((r as u8) & 0x40 != 0) as u8) << 6;
            }
            if bits_per_emission >= 8 {
                sym |= (((b as u8) & 0x40 != 0) as u8) << 7;
            }

            sym & mask
        }
        BitMapping::Hash => {
            // baseline: hash-mix the emission index + RGB bytes
            let mut x = map_seed ^ emission.rotate_left(17);
            for &b in rgb6.iter() {
                x ^= b as u64;
                x = x.wrapping_mul(0x9e3779b97f4a7c15);
                x ^= x >> 32;
            }
            (x as u8) & mask
        }
    }
}

fn ensure_symbol_stream_len(
    engine: &mut Engine,
    stream_syms: &mut Vec<u8>,
    need_len: usize,
    mapping: BitMapping,
    map_seed: u64,
    bits_per_emission: u8,
    search_emissions: u64,
    max_ticks: u64,
) -> bool {
    if stream_syms.len() >= need_len {
        return true;
    }

    while stream_syms.len() < need_len
        && (engine.stats.emissions as u64) < search_emissions
        && engine.stats.ticks < max_ticks
    {
        if let Some(tok) = engine.step() {
            let em = (engine.stats.emissions - 1) as u64;
            let rgb6 = tok.to_rgb_pair().to_bytes();
            let sym = map_symbol_bitfield(mapping, map_seed, em, &rgb6, bits_per_emission);
            stream_syms.push(sym);
        }
    }

    stream_syms.len() >= need_len
}

fn cmd_fit_xor_chunked_bitfield(a: FitXorChunkedArgs) -> anyhow::Result<()> {
    if a.mode != ApplyMode::Rgbpair {
        anyhow::bail!("--map bitfield requires --mode rgbpair");
    }
    if a.bits_per_emission == 0 || a.bits_per_emission > 8 {
        anyhow::bail!("--bits-per-emission must be in 1..=8");
    }
    if a.chunk_size == 0 {
        anyhow::bail!("--chunk-size must be >= 1");
    }
    if a.scan_step == 0 {
        anyhow::bail!("--scan-step must be >= 1");
    }

    let recipe = recipe_file::load_k8r(&a.recipe)?;
    let recipe_raw_len = std::fs::read(&a.recipe).map(|b| b.len()).unwrap_or(0usize);

    let target_bytes = std::fs::read(&a.target)?;
    if target_bytes.is_empty() {
        anyhow::bail!("target is empty");
    }

    // allow hex override for map_seed
    let seed = parse_seed_hex_opt(a.map_seed, &a.map_seed_hex)?;

    // Interpret the target bytes as a packed bitstream, then unpack into k-bit symbols.
    let bit_len: usize = target_bytes.len() * 8;
    let sym_count: usize =
        (bit_len + (a.bits_per_emission as usize) - 1) / (a.bits_per_emission as usize);

    let target_syms =
        bitpack::unpack_symbols(a.bits_per_emission, &target_bytes, sym_count)
            .map_err(|e| anyhow::anyhow!("{e}"))?;

    let mask = sym_mask(a.bits_per_emission);

    let mut engine = Engine::new(recipe)?;

    while (engine.stats.emissions as u64) < a.start_emission && engine.stats.ticks < a.max_ticks {
        let _ = engine.step();
        if (engine.stats.emissions as u64) >= a.search_emissions {
            break;
        }
    }

    let start_ticks = engine.stats.ticks;
    let start_em = engine.stats.emissions as u64;

    let mut stream_syms: Vec<u8> = Vec::new();
    stream_syms.reserve((a.search_emissions.saturating_sub(start_em)).min(500_000) as usize);

    while (engine.stats.emissions as u64) < a.search_emissions && engine.stats.ticks < a.max_ticks {
        if let Some(tok) = engine.step() {
            let em = (engine.stats.emissions - 1) as u64;
            let rgb6 = tok.to_rgb_pair().to_bytes();
            let sym = map_symbol_bitfield(a.bit_mapping, seed, em, &rgb6, a.bits_per_emission);
            stream_syms.push(sym & mask);
        }
    }

    let abs_stream_base_pos: u64 = a.start_emission;
    let total_n = target_syms.len();

    let mut tm_indices: Vec<u64> = Vec::with_capacity(total_n);
    let mut residual_syms: Vec<u8> = Vec::with_capacity(total_n);

    eprintln!(
        "--- fit-xor-chunked (bitfield) --- map=bitfield bits_per_emission={} bit_mapping={:?} map_seed={} (0x{:016x}) residual={:?} objective={:?} refine_topk={} lookahead={} chunk_size={} scan_step={} zstd_level={} target_bytes={} target_symbols={} stream_symbols={} base_pos={} start_emission={} end_emissions={} ticks={} delta_ticks={}",
        a.bits_per_emission,
        a.bit_mapping,
        seed,
        seed,
        a.residual,
        a.objective,
        a.refine_topk,
        a.lookahead,
        a.chunk_size,
        a.scan_step,
        a.zstd_level,
        target_bytes.len(),
        total_n,
        stream_syms.len(),
        abs_stream_base_pos,
        a.start_emission,
        (start_em + (stream_syms.len() as u64)),
        engine.stats.ticks,
        engine.stats.ticks.saturating_sub(start_ticks),
    );

    let mut prev_pos: Option<u64> = None;
    let mut chunk_idx: usize = 0;

    let mut off: usize = 0;
    while off < total_n {
        if a.max_chunks != 0 && chunk_idx >= a.max_chunks {
            break;
        }

        let remaining_total = total_n - off;
        let n = remaining_total.min(a.chunk_size);

        let min_pos: u64 = match prev_pos {
            None => abs_stream_base_pos,
            Some(p) => p.saturating_add(1),
        };

        let min_start: usize = (min_pos - abs_stream_base_pos) as usize;
        let max_start_cap = min_start.saturating_add(a.lookahead);

        let need_min = min_start.saturating_add(n);
        if need_min > stream_syms.len() {
            let ok = ensure_symbol_stream_len(
                &mut engine,
                &mut stream_syms,
                need_min,
                a.bit_mapping,
                seed,
                a.bits_per_emission,
                a.search_emissions,
                a.max_ticks,
            );
            if !ok {
                eprintln!(
                    "no room for chunk {}: need_len={} min_pos={} min_start={} stream_len={} chunk_len={} (writing partial)",
                    chunk_idx,
                    need_min,
                    min_pos,
                    min_start,
                    stream_syms.len(),
                    n
                );
                break;
            }
        }

        let need_finish_from_min = min_start.saturating_add(remaining_total);
        if need_finish_from_min > stream_syms.len() {
            let ok = ensure_symbol_stream_len(
                &mut engine,
                &mut stream_syms,
                need_finish_from_min,
                a.bit_mapping,
                seed,
                a.bits_per_emission,
                a.search_emissions,
                a.max_ticks,
            );
            if !ok {
                eprintln!(
                    "no room to finish from min_start for chunk {}: need_finish={} min_start={} stream_len={} remaining_total={} (writing partial)",
                    chunk_idx,
                    need_finish_from_min,
                    min_start,
                    stream_syms.len(),
                    remaining_total
                );
                break;
            }
        }

        let max_start_possible = if stream_syms.len() >= n { stream_syms.len() - n } else { 0 };
        let max_start_finish = stream_syms.len().saturating_sub(remaining_total);
        let max_start: usize = max_start_possible.min(max_start_cap).min(max_start_finish);

        if min_start > max_start {
            eprintln!(
                "no legal window for chunk {}: min_start={} max_start={} stream_len={} chunk_len={} remaining_total={} (writing partial)",
                chunk_idx,
                min_start,
                max_start,
                stream_syms.len(),
                n,
                remaining_total
            );
            break;
        }

        let mut scratch_resid: Vec<u8> = vec![0u8; n];

        let mut best_start_proxy: usize = min_start;
        let mut best_matches_proxy: u64 = 0;
        let mut best_proxy_score: usize = usize::MAX;

        let mut refine: Vec<(usize, usize, u64)> = Vec::new();

        let mut scanned: u64 = 0;

        let mut s0: usize = min_start;
        while s0 <= max_start {
            scanned += 1;

            let base_pos = abs_stream_base_pos + (s0 as u64);
            let mut matches: u64 = 0;

            for i in 0..n {
                let pred = stream_syms[s0 + i] & mask;
                let resid_b = make_residual_symbol(a.residual, pred, target_syms[off + i] & mask, mask);
                scratch_resid[i] = resid_b;
                if resid_b == 0 {
                    matches += 1;
                }
            }

            let jump_cost = tm_jump_cost(prev_pos, base_pos);

            let proxy_cost = (n as u64).saturating_sub(matches) as usize;
            let proxy_score = proxy_cost.saturating_add(jump_cost);

            if proxy_score < best_proxy_score
                || (proxy_score == best_proxy_score && s0 < best_start_proxy)
            {
                best_proxy_score = proxy_score;
                best_start_proxy = s0;
                best_matches_proxy = matches;
            }

            if a.objective == FitObjective::Matches && a.refine_topk != 0 {
                refine.push((proxy_score, s0, matches));
            }

            if a.objective == FitObjective::Zstd {
                let zlen = zstd_compress_len(&scratch_resid, a.zstd_level);
                let score = zlen.saturating_add(jump_cost);

                if score < best_proxy_score || (score == best_proxy_score && s0 < best_start_proxy) {
                    best_proxy_score = score;
                    best_start_proxy = s0;
                    best_matches_proxy = matches;
                }
            }

            s0 = s0.saturating_add(a.scan_step);
        }

        let mut best_start: usize = best_start_proxy;
        let mut best_matches: u64 = best_matches_proxy;
        let mut best_score: usize = best_proxy_score;
        let mut best_resid_zstd: usize = usize::MAX;

        if a.objective == FitObjective::Matches && a.refine_topk != 0 && !refine.is_empty() {
            refine.sort_by(|a1, b1| a1.0.cmp(&b1.0).then_with(|| a1.1.cmp(&b1.1)));
            if refine.len() > a.refine_topk {
                refine.truncate(a.refine_topk);
            }

            for &(_proxy_score, cand_s, cand_matches) in refine.iter() {
                let base_pos = abs_stream_base_pos + (cand_s as u64);
                let jump_cost = tm_jump_cost(prev_pos, base_pos);

                for i in 0..n {
                    scratch_resid[i] = make_residual_symbol(
                        a.residual,
                        stream_syms[cand_s + i] & mask,
                        target_syms[off + i] & mask,
                        mask,
                    );
                }

                let zlen = zstd_compress_len(&scratch_resid, a.zstd_level);
                let score = zlen.saturating_add(jump_cost);

                if score < best_score || (score == best_score && cand_s < best_start) {
                    best_score = score;
                    best_start = cand_s;
                    best_matches = cand_matches;
                    best_resid_zstd = zlen;
                }
            }

            if best_resid_zstd == usize::MAX {
                let base_pos = abs_stream_base_pos + (best_start as u64);
                let jump_cost = tm_jump_cost(prev_pos, base_pos);
                for i in 0..n {
                    scratch_resid[i] = make_residual_symbol(
                        a.residual,
                        stream_syms[best_start + i] & mask,
                        target_syms[off + i] & mask,
                        mask,
                    );
                }
                let zlen = zstd_compress_len(&scratch_resid, a.zstd_level);
                best_resid_zstd = zlen;
                best_score = zlen.saturating_add(jump_cost);
            }
        }

        let base_pos = abs_stream_base_pos + (best_start as u64);
        let jump_cost = tm_jump_cost(prev_pos, base_pos);

        for i in 0..n {
            let pos = base_pos + (i as u64);
            tm_indices.push(pos);
            residual_syms.push(make_residual_symbol(
                a.residual,
                stream_syms[best_start + i] & mask,
                target_syms[off + i] & mask,
                mask,
            ));
        }

        prev_pos = Some(base_pos + (n as u64) - 1);

        let printed_resid_metric = if a.objective == FitObjective::Zstd {
            let mut scratch: Vec<u8> = vec![0u8; n];
            for i in 0..n {
                scratch[i] = make_residual_symbol(
                    a.residual,
                    stream_syms[best_start + i] & mask,
                    target_syms[off + i] & mask,
                    mask,
                );
            }
            zstd_compress_len(&scratch, a.zstd_level)
        } else if best_resid_zstd != usize::MAX {
            best_resid_zstd
        } else {
            (n as u64).saturating_sub(best_matches) as usize
        };

        eprintln!(
            "chunk {:04} off_sym={} len_sym={} start_emission={} scanned_windows={} matches={}/{} ({:.2}%) jump_cost={} chunk_score={} chunk_resid_metric={}",
            chunk_idx,
            off,
            n,
            base_pos,
            scanned,
            best_matches,
            n,
            (best_matches as f64) * 100.0 / (n as f64),
            jump_cost,
            best_score,
            printed_resid_metric
        );

        off += n;
        chunk_idx += 1;
    }

    if tm_indices.len() != residual_syms.len() {
        anyhow::bail!(
            "internal: tm_indices/residual len mismatch: tm={} resid={}",
            tm_indices.len(),
            residual_syms.len()
        );
    }
    if tm_indices.is_empty() {
        anyhow::bail!("no output produced");
    }

    let produced_syms = residual_syms.len();
    if produced_syms != target_syms.len() {
        eprintln!(
            "note: partial output produced_symbols={} target_symbols={}",
            produced_syms,
            target_syms.len()
        );
    }

    let tm = TimingMap { indices: tm_indices };

    let tm_bytes = tm.encode_tm1();
    let tm_raw = tm_bytes.len();
    let tm_zstd = zstd_compress_len(&tm_bytes, a.zstd_level);

    // Write residual symbols with BF header, compute compressed len from packed payload
    let packed_resid = write_bitfield_residual(
        &a.out_residual,
        a.bits_per_emission,
        a.bit_mapping,
        target_bytes.len(),
        &residual_syms,
    )?;
    let resid_raw = packed_resid.len();
    let resid_zstd = zstd_compress_len(&packed_resid, a.zstd_level);

    // Plain baseline: zstd over the target packed-to-kbit representation (not raw bytes)
    let target_packed =
        bitpack::pack_symbols(a.bits_per_emission, &target_syms[..produced_syms])
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    let plain_zstd = zstd_compress_len(&target_packed, a.zstd_level);

    let effective_no_recipe = tm_zstd.saturating_add(resid_zstd);
    let effective_with_recipe = recipe_raw_len.saturating_add(effective_no_recipe);

    timemap::write_tm1(&a.out_timemap, &tm)?;

    eprintln!("--- scoreboard (bitfield) ---");
    eprintln!("recipe_raw_bytes           = {}", recipe_raw_len);
    eprintln!("plain_raw_bytes            = {}", target_bytes.len());
    eprintln!("plain_zstd_bytes           = {}", plain_zstd);
    eprintln!("tm1_raw_bytes              = {}", tm_raw);
    eprintln!("tm1_zstd_bytes             = {}", tm_zstd);
    eprintln!("resid_raw_bytes            = {}", resid_raw);
    eprintln!("resid_zstd_bytes           = {}", resid_zstd);
    eprintln!("effective_bytes_no_recipe  = {}", effective_no_recipe);
    eprintln!("effective_bytes_with_recipe= {}", effective_with_recipe);
    eprintln!(
        "delta_vs_plain_zstd_no_recipe  = {}",
        (effective_no_recipe as i64) - (plain_zstd as i64)
    );
    eprintln!(
        "delta_vs_plain_zstd_with_recipe= {}",
        (effective_with_recipe as i64) - (plain_zstd as i64)
    );

    Ok(())
}
