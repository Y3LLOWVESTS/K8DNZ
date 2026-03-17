use anyhow::{anyhow, Context, Result};
use clap::{Args, Subcommand, ValueEnum};

use k8dnz_apextrace::{
    analyze_key_against_bytes, branch_name, brute_force_best, generate_bytes, generate_quats,
    render_lattice, render_paths, render_subtree_stats, ApexKey, SearchCfg, SubtreeStats,
};

#[derive(Args, Debug)]
pub struct ApexTraceArgs {
    #[command(subcommand)]
    pub cmd: ApexTraceCmd,
}

#[derive(Subcommand, Debug)]
pub enum ApexTraceCmd {
    /// Create a deterministic ApexKey (.atk) from explicit parameters
    Pack(PackArgs),

    /// Inspect an ApexKey (.atk)
    Inspect(InspectArgs),

    /// Generate bytes or quaternary stream from an ApexKey (.atk)
    Gen(GenArgs),

    /// Brute-force search for a good ApexKey against target bytes
    Fit(FitArgs),

    /// Render 90-degree pyramid coordinates from an ApexKey or by fitting input on the fly
    Render(RenderArgs),

    /// Render subtree-conditioned stats against a target
    Stats(StatsArgs),

    /// Scan moving windows and report local-fit diagnostics
    WindowScan(WindowScanArgs),
}

#[derive(Args, Debug)]
pub struct PackArgs {
    #[arg(long)]
    pub byte_len: u64,

    #[arg(long, default_value_t = 0)]
    pub root_quadrant: u8,

    #[arg(long, default_value_t = 0)]
    pub root_seed: u64,

    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,

    #[arg(long)]
    pub out: String,
}

#[derive(Args, Debug)]
pub struct InspectArgs {
    #[arg(long)]
    pub atk: String,
}

#[derive(Args, Debug)]
pub struct GenArgs {
    #[arg(long)]
    pub atk: String,

    #[arg(long)]
    pub out: Option<String>,

    #[arg(long, default_value_t = false)]
    pub quats: bool,
}

#[derive(Args, Debug)]
pub struct FitArgs {
    #[arg(long = "in")]
    pub r#in: String,

    #[arg(long, default_value_t = 0)]
    pub seed_from: u64,

    #[arg(long, default_value_t = 4096)]
    pub seed_count: u64,

    #[arg(long, default_value_t = 1)]
    pub seed_step: u64,

    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,

    #[arg(long)]
    pub out_key: Option<String>,

    #[arg(long)]
    pub gen_out: Option<String>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum RenderMode {
    Lattice,
    Paths,
    Base,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
pub enum RenderFormat {
    Csv,
    Txt,
}

#[derive(Args, Debug)]
pub struct RenderArgs {
    #[arg(long)]
    pub atk: Option<String>,

    #[arg(long = "in")]
    pub r#in: Option<String>,

    #[arg(long, default_value_t = 0)]
    pub seed_from: u64,

    #[arg(long, default_value_t = 4096)]
    pub seed_count: u64,

    #[arg(long, default_value_t = 1)]
    pub seed_step: u64,

    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,

    #[arg(long)]
    pub out_key: Option<String>,

    #[arg(long)]
    pub max_quats: Option<u64>,

    #[arg(long, default_value_t = false)]
    pub active_only: bool,

    #[arg(long, value_enum, default_value_t = RenderMode::Lattice)]
    pub mode: RenderMode,

    #[arg(long, value_enum, default_value_t = RenderFormat::Csv)]
    pub format: RenderFormat,

    #[arg(long)]
    pub out: Option<String>,
}

#[derive(Args, Debug)]
pub struct StatsArgs {
    #[arg(long)]
    pub atk: Option<String>,

    #[arg(long = "in")]
    pub r#in: String,

    #[arg(long, default_value_t = 0)]
    pub seed_from: u64,

    #[arg(long, default_value_t = 4096)]
    pub seed_count: u64,

    #[arg(long, default_value_t = 1)]
    pub seed_step: u64,

    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,

    #[arg(long)]
    pub out_key: Option<String>,

    #[arg(long)]
    pub max_quats: Option<u64>,

    #[arg(long, default_value_t = false)]
    pub active_only: bool,

    #[arg(long, value_enum, default_value_t = RenderFormat::Csv)]
    pub format: RenderFormat,

    #[arg(long)]
    pub out: Option<String>,
}

#[derive(Args, Debug)]
pub struct WindowScanArgs {
    #[arg(long = "in")]
    pub r#in: String,

    #[arg(long, default_value_t = 16)]
    pub window_bytes: usize,

    #[arg(long, default_value_t = 1)]
    pub step_bytes: usize,

    #[arg(long)]
    pub max_windows: Option<usize>,

    #[arg(long, default_value_t = 0)]
    pub seed_from: u64,

    #[arg(long, default_value_t = 4096)]
    pub seed_count: u64,

    #[arg(long, default_value_t = 1)]
    pub seed_step: u64,

    #[arg(long, default_value_t = 1)]
    pub recipe_seed: u64,

    #[arg(long, value_enum, default_value_t = RenderFormat::Csv)]
    pub format: RenderFormat,

    #[arg(long)]
    pub out: Option<String>,
}

pub fn run(args: ApexTraceArgs) -> Result<()> {
    match args.cmd {
        ApexTraceCmd::Pack(a) => run_pack(a),
        ApexTraceCmd::Inspect(a) => run_inspect(a),
        ApexTraceCmd::Gen(a) => run_gen(a),
        ApexTraceCmd::Fit(a) => run_fit(a),
        ApexTraceCmd::Render(a) => run_render(a),
        ApexTraceCmd::Stats(a) => run_stats(a),
        ApexTraceCmd::WindowScan(a) => run_window_scan(a),
    }
}

fn run_pack(args: PackArgs) -> Result<()> {
    let key = ApexKey::new_dibit_v1(
        args.byte_len,
        args.root_quadrant,
        args.root_seed,
        args.recipe_seed,
    )?;
    let enc = key.encode()?;
    std::fs::write(&args.out, enc).with_context(|| format!("write {}", args.out))?;

    eprintln!(
        "apextrace pack ok: out={} bytes={} quat_len={} depth={} root_quadrant={} root_seed=0x{:016X} recipe_seed=0x{:016X}",
        args.out,
        key.byte_len,
        key.quat_len,
        key.depth,
        key.root_quadrant,
        key.root_seed,
        key.recipe_seed
    );

    Ok(())
}

fn run_inspect(args: InspectArgs) -> Result<()> {
    let bytes = std::fs::read(&args.atk).with_context(|| format!("read {}", args.atk))?;
    let key = ApexKey::decode(&bytes)?;

    println!("file={}", args.atk);
    println!("version={}", key.version);
    println!("mode={}", key.mode);
    println!("law_id={}", key.law_id);
    println!("byte_len={}", key.byte_len);
    println!("quat_len={}", key.quat_len);
    println!("depth={}", key.depth);
    println!("root_quadrant={}", key.root_quadrant);
    println!("root_seed=0x{:016X}", key.root_seed);
    println!("recipe_seed=0x{:016X}", key.recipe_seed);

    Ok(())
}

fn run_gen(args: GenArgs) -> Result<()> {
    let bytes = std::fs::read(&args.atk).with_context(|| format!("read {}", args.atk))?;
    let key = ApexKey::decode(&bytes)?;

    if args.quats {
        let quats = generate_quats(&key)?;
        let text = render_quats_ascii(&quats);
        match args.out {
            Some(path) => {
                std::fs::write(&path, text.as_bytes()).with_context(|| format!("write {}", path))?;
                eprintln!("apextrace gen ok: out={} mode=quats symbols={}", path, quats.len());
            }
            None => {
                println!("{text}");
            }
        }
    } else {
        let out_bytes = generate_bytes(&key)?;
        match args.out {
            Some(path) => {
                std::fs::write(&path, &out_bytes).with_context(|| format!("write {}", path))?;
                eprintln!("apextrace gen ok: out={} mode=bytes bytes={}", path, out_bytes.len());
            }
            None => {
                std::io::Write::write_all(&mut std::io::stdout(), &out_bytes)?;
            }
        }
    }

    Ok(())
}

fn run_fit(args: FitArgs) -> Result<()> {
    let target = std::fs::read(&args.r#in).with_context(|| format!("read {}", args.r#in))?;

    let cfg = SearchCfg {
        seed_from: args.seed_from,
        seed_count: args.seed_count,
        seed_step: args.seed_step,
        recipe_seed: args.recipe_seed,
    };

    let best = brute_force_best(&target, cfg)?;
    let diag = analyze_key_against_bytes(&best.key, &target)?;
    let pct = match_pct(diag.score.matches, diag.score.total);

    println!("input={}", args.r#in);
    println!("byte_len={}", best.key.byte_len);
    println!("quat_len={}", best.key.quat_len);
    println!("depth={}", best.key.depth);
    println!("best_root_quadrant={}", best.key.root_quadrant);
    println!("best_root_seed=0x{:016X}", best.key.root_seed);
    println!("recipe_seed=0x{:016X}", best.key.recipe_seed);
    println!("matches={}", diag.score.matches);
    println!("prefix={}", diag.score.prefix);
    println!("total={}", diag.score.total);
    println!("match_pct={:.6}", pct);
    println!("hamming={}", diag.score.hamming());
    println!("byte_matches={}", diag.byte_matches);
    println!("longest_run={}", diag.longest_run);
    println!("longest_run_start={}", diag.longest_run_start);
    println!("target_hist_1={}", diag.target_hist[0]);
    println!("target_hist_2={}", diag.target_hist[1]);
    println!("target_hist_3={}", diag.target_hist[2]);
    println!("target_hist_4={}", diag.target_hist[3]);
    println!("pred_hist_1={}", diag.pred_hist[0]);
    println!("pred_hist_2={}", diag.pred_hist[1]);
    println!("pred_hist_3={}", diag.pred_hist[2]);
    println!("pred_hist_4={}", diag.pred_hist[3]);

    if let Some(path) = args.out_key {
        let enc = best.key.encode()?;
        std::fs::write(&path, enc).with_context(|| format!("write {}", path))?;
        eprintln!("saved apex key: {}", path);
    }

    if let Some(path) = args.gen_out {
        let bytes = generate_bytes(&best.key)?;
        std::fs::write(&path, bytes).with_context(|| format!("write {}", path))?;
        eprintln!("saved generated bytes: {}", path);
    }

    Ok(())
}

fn run_render(args: RenderArgs) -> Result<()> {
    let key = resolve_key_for_render(&args)?;

    let body = match (args.mode, args.format) {
        (RenderMode::Lattice, RenderFormat::Csv) => {
            render_lattice_csv(&key, args.max_quats, args.active_only)?
        }
        (RenderMode::Lattice, RenderFormat::Txt) => {
            render_lattice_txt(&key, args.max_quats, args.active_only)?
        }
        (RenderMode::Paths, RenderFormat::Csv) => render_paths_csv(&key, args.max_quats)?,
        (RenderMode::Paths, RenderFormat::Txt) => render_paths_txt(&key, args.max_quats)?,
        (RenderMode::Base, RenderFormat::Csv) => render_base_csv(&key, args.max_quats)?,
        (RenderMode::Base, RenderFormat::Txt) => render_base_txt(&key, args.max_quats)?,
    };

    write_or_print(args.out.as_deref(), &body)?;
    if let Some(path) = args.out.as_deref() {
        eprintln!(
            "apextrace render ok: out={} mode={:?} format={:?}",
            path, args.mode, args.format
        );
    }

    Ok(())
}

fn run_stats(args: StatsArgs) -> Result<()> {
    let target = std::fs::read(&args.r#in).with_context(|| format!("read {}", args.r#in))?;
    let key = resolve_key_for_target(
        args.atk.as_deref(),
        &target,
        args.seed_from,
        args.seed_count,
        args.seed_step,
        args.recipe_seed,
        args.out_key.as_deref(),
        "stats",
    )?;

    let body = match args.format {
        RenderFormat::Csv => render_stats_csv(&key, &target, args.max_quats, args.active_only)?,
        RenderFormat::Txt => render_stats_txt(&key, &target, args.max_quats, args.active_only)?,
    };

    write_or_print(args.out.as_deref(), &body)?;
    if let Some(path) = args.out.as_deref() {
        eprintln!("apextrace stats ok: out={} format={:?}", path, args.format);
    }

    Ok(())
}

fn run_window_scan(args: WindowScanArgs) -> Result<()> {
    if args.window_bytes == 0 {
        return Err(anyhow!("window-scan requires --window-bytes >= 1"));
    }
    if args.step_bytes == 0 {
        return Err(anyhow!("window-scan requires --step-bytes >= 1"));
    }

    let bytes = std::fs::read(&args.r#in).with_context(|| format!("read {}", args.r#in))?;
    if bytes.len() < args.window_bytes {
        return Err(anyhow!(
            "window-scan input is smaller than one window: bytes={} window_bytes={}",
            bytes.len(),
            args.window_bytes
        ));
    }

    let cfg = SearchCfg {
        seed_from: args.seed_from,
        seed_count: args.seed_count,
        seed_step: args.seed_step,
        recipe_seed: args.recipe_seed,
    };

    let mut rows = Vec::new();
    let mut start = 0usize;
    let mut window_idx = 0usize;

    while start + args.window_bytes <= bytes.len() {
        if let Some(max_windows) = args.max_windows {
            if rows.len() >= max_windows {
                break;
            }
        }

        let end = start + args.window_bytes;
        let window = &bytes[start..end];
        let best = brute_force_best(window, cfg)?;
        let diag = analyze_key_against_bytes(&best.key, window)?;
        let stats = render_subtree_stats(&best.key, window, None)?;
        let hot = pick_hot_node(&stats);

        rows.push(WindowScanRow {
            window_idx,
            byte_start: start,
            byte_end: end,
            byte_len: args.window_bytes,
            quat_len: best.key.quat_len,
            best_root_quadrant: best.key.root_quadrant,
            best_root_seed: best.key.root_seed,
            recipe_seed: best.key.recipe_seed,
            matches: diag.score.matches,
            prefix: diag.score.prefix,
            total: diag.score.total,
            byte_matches: diag.byte_matches,
            longest_run: diag.longest_run,
            longest_run_start: diag.longest_run_start,
            target_hist: diag.target_hist,
            pred_hist: diag.pred_hist,
            hot_row: hot.map(|s| s.row).unwrap_or(0),
            hot_k: hot.map(|s| s.k).unwrap_or(0),
            hot_subtree_size: hot.map(|s| s.subtree_size).unwrap_or(0),
            hot_matches: hot.map(|s| s.matches).unwrap_or(0),
            hot_match_rate_ppm: hot.map(|s| s.match_rate_ppm()).unwrap_or(0),
            hot_match_excess_ppm: hot.map(|s| s.match_excess_ppm()).unwrap_or(0),
        });

        start = start.saturating_add(args.step_bytes);
        window_idx = window_idx.saturating_add(1);
    }

    let body = match args.format {
        RenderFormat::Csv => window_scan_csv(&rows),
        RenderFormat::Txt => window_scan_txt(&rows),
    };

    write_or_print(args.out.as_deref(), &body)?;

    if let Some(best) = rows.iter().max_by_key(|row| (row.matches, row.longest_run, row.byte_matches)) {
        eprintln!(
            "apextrace window-scan ok: windows={} best_idx={} byte_range={}..{} matches={} total={} match_pct={:.6} longest_run={} hot_node=({}, {}) hot_match_excess_vs_random={:.6}",
            rows.len(),
            best.window_idx,
            best.byte_start,
            best.byte_end,
            best.matches,
            best.total,
            match_pct(best.matches, best.total),
            best.longest_run,
            best.hot_row,
            best.hot_k,
            signed_percent_from_ppm(best.hot_match_excess_ppm),
        );
    } else {
        eprintln!("apextrace window-scan ok: windows=0");
    }

    Ok(())
}

fn resolve_key_for_render(args: &RenderArgs) -> Result<ApexKey> {
    match (&args.atk, &args.r#in) {
        (Some(_), Some(_)) => Err(anyhow!("render accepts either --atk or --in, not both")),
        (None, None) => Err(anyhow!("render requires either --atk or --in")),
        (Some(path), None) => {
            let bytes = std::fs::read(path).with_context(|| format!("read {}", path))?;
            ApexKey::decode(&bytes).map_err(Into::into)
        }
        (None, Some(path)) => {
            let target = std::fs::read(path).with_context(|| format!("read {}", path))?;
            resolve_key_for_target(
                None,
                &target,
                args.seed_from,
                args.seed_count,
                args.seed_step,
                args.recipe_seed,
                args.out_key.as_deref(),
                "render",
            )
        }
    }
}

fn resolve_key_for_target(
    atk: Option<&str>,
    target: &[u8],
    seed_from: u64,
    seed_count: u64,
    seed_step: u64,
    recipe_seed: u64,
    out_key: Option<&str>,
    verb: &str,
) -> Result<ApexKey> {
    match atk {
        Some(path) => {
            let bytes = std::fs::read(path).with_context(|| format!("read {}", path))?;
            ApexKey::decode(&bytes).map_err(Into::into)
        }
        None => {
            let cfg = SearchCfg {
                seed_from,
                seed_count,
                seed_step,
                recipe_seed,
            };

            let best = brute_force_best(target, cfg)?;
            let pct = match_pct(best.score.matches, best.score.total);

            eprintln!(
                "apextrace {} fit: byte_len={} quat_len={} depth={} root_quadrant={} root_seed=0x{:016X} recipe_seed=0x{:016X} matches={} total={} match_pct={:.6}",
                verb,
                best.key.byte_len,
                best.key.quat_len,
                best.key.depth,
                best.key.root_quadrant,
                best.key.root_seed,
                best.key.recipe_seed,
                best.score.matches,
                best.score.total,
                pct
            );

            if let Some(out_key) = out_key {
                let enc = best.key.encode()?;
                std::fs::write(out_key, enc).with_context(|| format!("write {}", out_key))?;
                eprintln!("saved apex key: {}", out_key);
            }

            Ok(best.key)
        }
    }
}

fn render_lattice_csv(key: &ApexKey, max_quats: Option<u64>, active_only: bool) -> Result<String> {
    let points = render_lattice(key, max_quats)?;
    let mut out = String::from("row,k,x,y,visits,leaf_span,active\n");

    for p in points {
        if active_only && p.visits == 0 {
            continue;
        }
        out.push_str(&format!(
            "{},{},{},{},{},{},{}\n",
            p.row,
            p.k,
            p.x,
            p.y,
            p.visits,
            p.leaf_span,
            if p.visits > 0 { 1 } else { 0 }
        ));
    }

    Ok(out)
}

fn render_lattice_txt(key: &ApexKey, max_quats: Option<u64>, active_only: bool) -> Result<String> {
    let points = render_lattice(key, max_quats)?;
    let mut out = String::new();

    for p in points {
        if active_only && p.visits == 0 {
            continue;
        }
        out.push_str(&format!(
            "row={} k={} x={} y={} visits={} leaf_span={}\n",
            p.row, p.k, p.x, p.y, p.visits, p.leaf_span
        ));
    }

    Ok(out)
}

fn render_paths_csv(key: &ApexKey, max_quats: Option<u64>) -> Result<String> {
    let (points, _) = render_paths(key, max_quats)?;
    let mut out = String::from("leaf,step,row,k,x,y,branch,q,u_hex,quat\n");

    for p in points {
        let quat = p.quat.map(|v| v.to_string()).unwrap_or_default();
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},0x{:016X},{}\n",
            p.leaf,
            p.step,
            p.row,
            p.k,
            p.x,
            p.y,
            branch_name(p.branch),
            p.q,
            p.u,
            quat
        ));
    }

    Ok(out)
}

fn render_paths_txt(key: &ApexKey, max_quats: Option<u64>) -> Result<String> {
    let (points, _) = render_paths(key, max_quats)?;
    let mut out = String::new();

    for p in points {
        match p.quat {
            Some(quat) => out.push_str(&format!(
                "leaf={} step={} row={} k={} x={} y={} branch={} q={} u=0x{:016X} quat={}\n",
                p.leaf,
                p.step,
                p.row,
                p.k,
                p.x,
                p.y,
                branch_name(p.branch),
                p.q,
                p.u,
                quat
            )),
            None => out.push_str(&format!(
                "leaf={} step={} row={} k={} x={} y={} branch={} q={} u=0x{:016X}\n",
                p.leaf,
                p.step,
                p.row,
                p.k,
                p.x,
                p.y,
                branch_name(p.branch),
                p.q,
                p.u
            )),
        }
    }

    Ok(out)
}

fn render_base_csv(key: &ApexKey, max_quats: Option<u64>) -> Result<String> {
    let (_, labels) = render_paths(key, max_quats)?;
    let mut out = String::from("leaf,row,k,x,y,quat\n");

    for b in labels {
        out.push_str(&format!(
            "{},{},{},{},{},{}\n",
            b.leaf, b.row, b.k, b.x, b.y, b.quat
        ));
    }

    Ok(out)
}

fn render_base_txt(key: &ApexKey, max_quats: Option<u64>) -> Result<String> {
    let (_, labels) = render_paths(key, max_quats)?;
    let mut out = String::new();

    for b in labels {
        out.push_str(&format!(
            "leaf={} row={} k={} x={} y={} quat={}\n",
            b.leaf, b.row, b.k, b.x, b.y, b.quat
        ));
    }

    Ok(out)
}

fn render_stats_csv(
    key: &ApexKey,
    target: &[u8],
    max_quats: Option<u64>,
    active_only: bool,
) -> Result<String> {
    let stats = render_subtree_stats(key, target, max_quats)?;
    let mut out = String::from(
        "row,k,x,y,subtree_size,leaf_range_start,leaf_range_end,target_hist_1,target_hist_2,target_hist_3,target_hist_4,pred_hist_1,pred_hist_2,pred_hist_3,pred_hist_4,matches,mismatches,match_rate,match_excess_vs_random,target_purity,pred_purity,target_entropy,pred_entropy,active\n",
    );

    for s in stats {
        if active_only && !s.active() {
            continue;
        }

        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6},{:.6},{}\n",
            s.row,
            s.k,
            s.x,
            s.y,
            s.subtree_size,
            s.leaf_range_start,
            s.leaf_range_end,
            s.target_hist[0],
            s.target_hist[1],
            s.target_hist[2],
            s.target_hist[3],
            s.pred_hist[0],
            s.pred_hist[1],
            s.pred_hist[2],
            s.pred_hist[3],
            s.matches,
            s.mismatches,
            percent_from_ppm(s.match_rate_ppm()),
            signed_percent_from_ppm(s.match_excess_ppm()),
            percent_from_ppm(s.target_purity_ppm()),
            percent_from_ppm(s.pred_purity_ppm()),
            s.target_entropy_bits(),
            s.pred_entropy_bits(),
            if s.active() { 1 } else { 0 }
        ));
    }

    Ok(out)
}

fn render_stats_txt(
    key: &ApexKey,
    target: &[u8],
    max_quats: Option<u64>,
    active_only: bool,
) -> Result<String> {
    let stats = render_subtree_stats(key, target, max_quats)?;
    let mut out = String::new();

    for s in stats {
        if active_only && !s.active() {
            continue;
        }

        out.push_str(&format!(
            "row={} k={} x={} y={} subtree_size={} leaf_range_start={} leaf_range_end={} target_hist=[{},{},{},{}] pred_hist=[{},{},{},{}] matches={} mismatches={} match_rate={:.6} match_excess_vs_random={:.6} target_purity={:.6} pred_purity={:.6} target_entropy={:.6} pred_entropy={:.6}\n",
            s.row,
            s.k,
            s.x,
            s.y,
            s.subtree_size,
            s.leaf_range_start,
            s.leaf_range_end,
            s.target_hist[0],
            s.target_hist[1],
            s.target_hist[2],
            s.target_hist[3],
            s.pred_hist[0],
            s.pred_hist[1],
            s.pred_hist[2],
            s.pred_hist[3],
            s.matches,
            s.mismatches,
            percent_from_ppm(s.match_rate_ppm()),
            signed_percent_from_ppm(s.match_excess_ppm()),
            percent_from_ppm(s.target_purity_ppm()),
            percent_from_ppm(s.pred_purity_ppm()),
            s.target_entropy_bits(),
            s.pred_entropy_bits(),
        ));
    }

    Ok(out)
}

#[derive(Clone, Debug)]
struct WindowScanRow {
    window_idx: usize,
    byte_start: usize,
    byte_end: usize,
    byte_len: usize,
    quat_len: u64,
    best_root_quadrant: u8,
    best_root_seed: u64,
    recipe_seed: u64,
    matches: u64,
    prefix: u64,
    total: u64,
    byte_matches: u64,
    longest_run: u64,
    longest_run_start: u64,
    target_hist: [u64; 4],
    pred_hist: [u64; 4],
    hot_row: u16,
    hot_k: u16,
    hot_subtree_size: u64,
    hot_matches: u64,
    hot_match_rate_ppm: u64,
    hot_match_excess_ppm: i64,
}

fn window_scan_csv(rows: &[WindowScanRow]) -> String {
    let mut out = String::from(
        "window_idx,byte_start,byte_end,byte_len,quat_len,best_root_quadrant,best_root_seed_hex,recipe_seed_hex,matches,prefix,total,match_pct,byte_matches,longest_run,longest_run_start,target_hist_1,target_hist_2,target_hist_3,target_hist_4,pred_hist_1,pred_hist_2,pred_hist_3,pred_hist_4,hot_row,hot_k,hot_subtree_size,hot_matches,hot_match_rate,hot_match_excess_vs_random\n",
    );

    for row in rows {
        out.push_str(&format!(
            "{},{},{},{},{},{},0x{:016X},0x{:016X},{},{},{},{:.6},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.6},{:.6}\n",
            row.window_idx,
            row.byte_start,
            row.byte_end,
            row.byte_len,
            row.quat_len,
            row.best_root_quadrant,
            row.best_root_seed,
            row.recipe_seed,
            row.matches,
            row.prefix,
            row.total,
            match_pct(row.matches, row.total),
            row.byte_matches,
            row.longest_run,
            row.longest_run_start,
            row.target_hist[0],
            row.target_hist[1],
            row.target_hist[2],
            row.target_hist[3],
            row.pred_hist[0],
            row.pred_hist[1],
            row.pred_hist[2],
            row.pred_hist[3],
            row.hot_row,
            row.hot_k,
            row.hot_subtree_size,
            row.hot_matches,
            percent_from_ppm(row.hot_match_rate_ppm),
            signed_percent_from_ppm(row.hot_match_excess_ppm),
        ));
    }

    out
}

fn window_scan_txt(rows: &[WindowScanRow]) -> String {
    let mut out = String::new();

    for row in rows {
        out.push_str(&format!(
            "window_idx={} byte_start={} byte_end={} byte_len={} quat_len={} best_root_quadrant={} best_root_seed=0x{:016X} recipe_seed=0x{:016X} matches={} prefix={} total={} match_pct={:.6} byte_matches={} longest_run={} longest_run_start={} target_hist=[{},{},{},{}] pred_hist=[{},{},{},{}] hot_row={} hot_k={} hot_subtree_size={} hot_matches={} hot_match_rate={:.6} hot_match_excess_vs_random={:.6}\n",
            row.window_idx,
            row.byte_start,
            row.byte_end,
            row.byte_len,
            row.quat_len,
            row.best_root_quadrant,
            row.best_root_seed,
            row.recipe_seed,
            row.matches,
            row.prefix,
            row.total,
            match_pct(row.matches, row.total),
            row.byte_matches,
            row.longest_run,
            row.longest_run_start,
            row.target_hist[0],
            row.target_hist[1],
            row.target_hist[2],
            row.target_hist[3],
            row.pred_hist[0],
            row.pred_hist[1],
            row.pred_hist[2],
            row.pred_hist[3],
            row.hot_row,
            row.hot_k,
            row.hot_subtree_size,
            row.hot_matches,
            percent_from_ppm(row.hot_match_rate_ppm),
            signed_percent_from_ppm(row.hot_match_excess_ppm),
        ));
    }

    out
}

fn pick_hot_node(stats: &[SubtreeStats]) -> Option<&SubtreeStats> {
    let mut best: Option<&SubtreeStats> = None;

    for stat in stats {
        if !stat.active() || stat.row == 0 {
            continue;
        }

        match best {
            None => best = Some(stat),
            Some(current) => {
                let cand_key = (
                    stat.match_excess_ppm(),
                    stat.matches,
                    stat.row,
                    stat.subtree_size,
                    std::cmp::Reverse(stat.k),
                );
                let curr_key = (
                    current.match_excess_ppm(),
                    current.matches,
                    current.row,
                    current.subtree_size,
                    std::cmp::Reverse(current.k),
                );

                if cand_key > curr_key {
                    best = Some(stat);
                }
            }
        }
    }

    best
}

fn render_quats_ascii(quats: &[u8]) -> String {
    let mut out = String::with_capacity(quats.len());
    for &q in quats {
        out.push(match q {
            1 => '1',
            2 => '2',
            3 => '3',
            4 => '4',
            _ => '?',
        });
    }
    out
}

fn write_or_print(out: Option<&str>, body: &str) -> Result<()> {
    match out {
        Some(path) => {
            std::fs::write(path, body.as_bytes()).with_context(|| format!("write {}", path))?;
        }
        None => {
            print!("{body}");
        }
    }
    Ok(())
}

fn match_pct(matches: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        (matches as f64) * 100.0 / (total as f64)
    }
}

fn percent_from_ppm(ppm: u64) -> f64 {
    (ppm as f64) / 10_000.0
}

fn signed_percent_from_ppm(ppm: i64) -> f64 {
    (ppm as f64) / 10_000.0
}
