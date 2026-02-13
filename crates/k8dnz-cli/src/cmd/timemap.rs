// crates/k8dnz-cli/src/cmd/timemap.rs

use clap::{Args, Subcommand, ValueEnum};
use k8dnz_core::signal::timing_map::TimingMap;
use k8dnz_core::Engine;

use crate::io::{recipe_file, timemap};

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum ApplyMode {
    Pair,
    Rgbpair,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum MapMode {
    None,
    Splitmix64,
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
    Apply(ApplyArgs),
    Fit(FitArgs),
    FitXor(FitXorArgs),
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

    #[arg(long, default_value_t = 2_000_000)]
    pub search_emissions: u64,

    #[arg(long, default_value_t = 80_000_000)]
    pub max_ticks: u64,

    #[arg(long, default_value_t = 0)]
    pub start_emission: u64,
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

    #[arg(long, default_value_t = 80_000_000)]
    pub max_ticks: u64,
}

pub fn run(args: TimemapArgs) -> anyhow::Result<()> {
    match args.cmd {
        TimemapCmd::Make(a) => cmd_make(a),
        TimemapCmd::Inspect(a) => cmd_inspect(a),
        TimemapCmd::Apply(a) => cmd_apply(a),
        TimemapCmd::Fit(a) => cmd_fit(a),
        TimemapCmd::FitXor(a) => cmd_fit_xor(a),
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
    let target = std::fs::read(&a.target)?;

    if target.is_empty() {
        anyhow::bail!("target is empty");
    }

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
                ApplyMode::Rgbpair => {
                    let rgb6 = tok.to_rgb_pair().to_bytes();
                    stream.extend_from_slice(&rgb6);
                }
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

    let mut best_start: usize = 0;
    let mut best_matches: u64 = 0;

    let abs_stream_base_pos: u64 = a.start_emission * bytes_per_emission;

    for s in 0..=max_start {
        let mut m: u64 = 0;
        let base_pos = abs_stream_base_pos + (s as u64);
        for i in 0..n {
            let pos = base_pos + (i as u64);
            let cand = map_byte(a.map, a.map_seed, pos, stream[s + i]);
            if cand == target[i] {
                m += 1;
            }
        }
        if m > best_matches {
            best_matches = m;
            best_start = s;
            if best_matches == n as u64 {
                break;
            }
        }
    }

    let abs_win_start_pos: u64 = abs_stream_base_pos + (best_start as u64);

    let indices: Vec<u64> = (0..(n as u64)).map(|i| abs_win_start_pos + i).collect();
    let tm = TimingMap { indices };

    let mut residual: Vec<u8> = Vec::with_capacity(n);
    for i in 0..n {
        let pos = abs_win_start_pos + (i as u64);
        let mapped = map_byte(a.map, a.map_seed, pos, stream[best_start + i]);
        residual.push(mapped ^ target[i]);
    }

    timemap::write_tm1(&a.out_timemap, &tm)?;
    std::fs::write(&a.out_residual, &residual)?;

    let (win_emission, win_lane) = match a.mode {
        ApplyMode::Pair => (abs_win_start_pos, 0),
        ApplyMode::Rgbpair => (abs_win_start_pos / 6, (abs_win_start_pos % 6)),
    };

    eprintln!(
        "timemap fit-xor ok: mode={:?} map={:?} map_seed={} tm_out={} resid_out={} target_bytes={} matches={}/{} ({:.4}%) window_start_pos={} (emission={} lane={}) scanned_emissions={} stream_bytes={} ticks={} delta_ticks={}",
        a.mode,
        a.map,
        a.map_seed,
        a.out_timemap,
        a.out_residual,
        n,
        best_matches,
        n,
        (best_matches as f64) * 100.0 / (n as f64),
        abs_win_start_pos,
        win_emission,
        win_lane,
        (start_em + (stream.len() as u64 / bytes_per_emission)),
        stream.len(),
        engine.stats.ticks,
        engine.stats.ticks.saturating_sub(start_ticks),
    );

    Ok(())
}

fn cmd_reconstruct(a: ReconstructArgs) -> anyhow::Result<()> {
    let recipe = recipe_file::load_k8r(&a.recipe)?;
    let tm = timemap::read_tm1(&a.timemap)?;
    let resid = std::fs::read(&a.residual)?;

    if tm.indices.is_empty() {
        anyhow::bail!("timemap empty");
    }
    if resid.len() != tm.indices.len() {
        anyhow::bail!(
            "residual length mismatch: resid={} tm_len={}",
            resid.len(),
            tm.indices.len()
        );
    }

    let mut engine = Engine::new(recipe)?;

    let raw_stream = collect_flat_stream_bytes(&mut engine, &tm, a.max_ticks, a.mode)?;

    let mut out: Vec<u8> = Vec::with_capacity(raw_stream.len());
    for i in 0..raw_stream.len() {
        let pos = tm.indices[i];
        let mapped = map_byte(a.map, a.map_seed, pos, raw_stream[i]);
        out.push(mapped ^ resid[i]);
    }

    std::fs::write(&a.out, &out)?;
    eprintln!(
        "reconstruct ok: mode={:?} map={:?} map_seed={} out={} bytes={} ticks={} emissions={}",
        a.mode,
        a.map,
        a.map_seed,
        a.out,
        out.len(),
        engine.stats.ticks,
        engine.stats.emissions
    );
    Ok(())
}

fn map_byte(mode: MapMode, seed: u64, pos: u64, raw: u8) -> u8 {
    match mode {
        MapMode::None => raw,
        MapMode::Splitmix64 => {
            let k = splitmix64(seed ^ pos) as u8;
            raw ^ k
        }
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

fn collect_flat_stream_bytes(
    engine: &mut Engine,
    tm: &TimingMap,
    max_ticks: u64,
    mode: ApplyMode,
) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(tm.indices.len());
    let mut want_i: usize = 0;

    let last_pos = tm.indices[tm.indices.len() - 1];

    let bytes_per_emission: u64 = match mode {
        ApplyMode::Pair => 1,
        ApplyMode::Rgbpair => 6,
    };

    let first_pos = tm.indices[0];

    if bytes_per_emission > 0 {
        let target_em = first_pos / bytes_per_emission;
        while (engine.stats.emissions as u64) < target_em && engine.stats.ticks < max_ticks {
            let _ = engine.step();
        }
    }

    while engine.stats.ticks < max_ticks {
        if let Some(tok) = engine.step() {
            let em_idx = (engine.stats.emissions - 1) as u64;
            let base = em_idx * bytes_per_emission;

            if base > last_pos {
                break;
            }

            match mode {
                ApplyMode::Pair => {
                    if want_i < tm.indices.len() && tm.indices[want_i] == em_idx {
                        out.push(tok.pack_byte());
                        want_i += 1;
                    }
                }
                ApplyMode::Rgbpair => {
                    let rgb6 = tok.to_rgb_pair().to_bytes();
                    while want_i < tm.indices.len() {
                        let p = tm.indices[want_i];
                        if p < base {
                            anyhow::bail!(
                                "timemap indices not sorted? want_pos={} < base={}",
                                p,
                                base
                            );
                        }
                        if p >= base + 6 {
                            break;
                        }
                        let lane = (p - base) as usize;
                        out.push(rgb6[lane]);
                        want_i += 1;
                    }
                }
            }

            if want_i == tm.indices.len() {
                break;
            }
        }
    }

    if want_i != tm.indices.len() {
        anyhow::bail!(
            "reconstruct short: need {} selections, got {} (mode={:?}, ticks={}, emissions={}, last_pos={})",
            tm.indices.len(),
            want_i,
            mode,
            engine.stats.ticks,
            engine.stats.emissions,
            last_pos
        );
    }

    Ok(out)
}

fn collect_pair_bytes(engine: &mut Engine, tm: &TimingMap, max_ticks: u64) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(tm.indices.len());
    let mut want_i: usize = 0;

    let last = tm.indices[tm.indices.len() - 1];

    while (engine.stats.emissions as u64) <= last && engine.stats.ticks < max_ticks {
        if let Some(tok) = engine.step() {
            let idx = (engine.stats.emissions - 1) as u64;
            if idx == tm.indices[want_i] {
                out.push(tok.pack_byte());
                want_i += 1;
                if want_i == tm.indices.len() {
                    break;
                }
            }
        }
    }

    if want_i != tm.indices.len() {
        anyhow::bail!(
            "timemap apply short: need {} selections, got {} (ticks={}, emissions={})",
            tm.indices.len(),
            want_i,
            engine.stats.ticks,
            engine.stats.emissions
        );
    }

    Ok(out)
}

fn collect_rgbpair_bytes(engine: &mut Engine, tm: &TimingMap, max_ticks: u64) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(tm.indices.len() * 6);
    let mut want_i: usize = 0;

    let last = tm.indices[tm.indices.len() - 1];

    while (engine.stats.emissions as u64) <= last && engine.stats.ticks < max_ticks {
        if let Some(tok) = engine.step() {
            let idx = (engine.stats.emissions - 1) as u64;
            if idx == tm.indices[want_i] {
                let rgb = tok.to_rgb_pair().to_bytes();
                out.extend_from_slice(&rgb);
                want_i += 1;
                if want_i == tm.indices.len() {
                    break;
                }
            }
        }
    }

    if want_i != tm.indices.len() {
        anyhow::bail!(
            "timemap apply short: need {} selections, got {} (ticks={}, emissions={})",
            tm.indices.len(),
            want_i,
            engine.stats.ticks,
            engine.stats.emissions
        );
    }

    Ok(out)
}
