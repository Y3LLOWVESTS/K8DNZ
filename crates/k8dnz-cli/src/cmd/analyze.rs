use clap::Args;
use std::io::Cursor;

#[derive(Args, Debug)]
pub struct AnalyzeArgs {
    /// Input file path to analyze as raw bytes
    #[arg(long)]
    pub r#in: String,

    /// Show the top N most frequent bytes
    #[arg(long, default_value_t = 16)]
    pub top: usize,

    /// Also report zstd compressed size (as a real-world compressibility scoreboard)
    #[arg(long, default_value_t = true)]
    pub zstd: bool,

    /// Zstd compression level (1..=22 typical). Higher is slower.
    #[arg(long, default_value_t = 3)]
    pub zstd_level: i32,
}

pub fn run(args: AnalyzeArgs) -> anyhow::Result<()> {
    let bytes = std::fs::read(&args.r#in)?;
    let n = bytes.len() as u64;

    let mut h = [0u64; 256];
    for &b in &bytes {
        h[b as usize] += 1;
    }

    let distinct = h.iter().filter(|&&c| c > 0).count();
    let (minc, maxc) = min_max_256(&h);
    let entropy = entropy_bits_256(&h, n);

    // Build ranking for top bytes
    let mut rows: Vec<(u8, u64)> = (0u8..=255u8)
        .map(|b| (b, h[b as usize]))
        .filter(|&(_b, c)| c > 0)
        .collect();

    rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    eprintln!("--- analyze ---");
    eprintln!("file            = {}", args.r#in);
    eprintln!("bytes           = {}", n);
    eprintln!("distinct_bytes  = {}/256", distinct);
    eprintln!("min_count       = {}", minc);
    eprintln!("max_count       = {}", maxc);
    eprintln!("entropy_bits    = {:.6} (max 8.000000)", entropy);

    if args.zstd {
        let z = zstd_size(&bytes, args.zstd_level)?;
        let ratio = if z == 0 { 0.0 } else { (n as f64) / (z as f64) };
        eprintln!("--- zstd ---");
        eprintln!("zstd_level      = {}", args.zstd_level);
        eprintln!("zstd_bytes      = {}", z);
        eprintln!("ratio_raw/zstd  = {:.4}x", ratio);
    }

    let topn = args.top.min(rows.len());
    eprintln!("--- top {} bytes ---", topn);
    for (i, (b, c)) in rows.iter().take(topn).enumerate() {
        let pct = if n == 0 { 0.0 } else { (*c as f64) * 100.0 / (n as f64) };
        eprintln!(
            "#{:>2} byte=0x{:02X} ({:>3}) count={} ({:.3}%)",
            i + 1,
            b,
            b,
            c,
            pct
        );
    }

    Ok(())
}

fn zstd_size(bytes: &[u8], level: i32) -> anyhow::Result<usize> {
    // Deterministic given bytes+level; good enough for a “scoreboard”.
    let out = zstd::stream::encode_all(Cursor::new(bytes), level)?;
    Ok(out.len())
}

fn min_max_256(h: &[u64; 256]) -> (u64, u64) {
    let mut min = u64::MAX;
    let mut max = 0u64;
    for &v in h.iter() {
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
    }
    if min == u64::MAX {
        min = 0;
    }
    (min, max)
}

fn entropy_bits_256(h: &[u64; 256], total: u64) -> f64 {
    if total == 0 {
        return 0.0;
    }
    let mut ent = 0.0;
    for &c in h.iter() {
        if c == 0 {
            continue;
        }
        let p = (c as f64) / (total as f64);
        ent -= p * p.log2();
    }
    ent
}
