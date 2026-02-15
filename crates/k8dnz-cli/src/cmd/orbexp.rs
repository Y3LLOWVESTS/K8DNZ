// crates/k8dnz-cli/src/cmd/orbexp.rs

use clap::{Args, Subcommand};
use k8dnz_core::orbexp::{bitlen_u64, compute_first_meet, derive_steps, DeriveMode, OrbParams};

#[derive(Args)]
pub struct OrbExpArgs {
    #[command(subcommand)]
    pub cmd: OrbExpCmd,
}

#[derive(Subcommand)]
pub enum OrbExpCmd {
    /// Scan a file in fixed-size bit blocks and compute meet-time stats (CSV output).
    Blockscan(BlockScanArgs),

    /// Compute a single block (hex) meet-time (debug-friendly).
    One(OneArgs),
}

#[derive(Args)]
pub struct BlockScanArgs {
    /// Input file path
    #[arg(long = "in")]
    pub r#in: String,

    /// Block size in bits (e.g., 64 / 100 / 128)
    #[arg(long, default_value_t = 128)]
    pub block_bits: usize,

    /// Modular circle size (MOD). Use primes to reduce accidental structure.
    #[arg(long, default_value_t = 4_294_967_291u64)]
    pub r#mod: u64,

    /// Base constant P used to derive steps (decimal or 0x... hex).
    #[arg(long, default_value = "0x243f6a8885a308d3")]
    pub p: String,

    /// Derivation mode: int | crc32 | decpairs
    #[arg(long, default_value = "int")]
    pub derive: String,

    /// Limit number of blocks processed (0 = no limit)
    #[arg(long, default_value_t = 0)]
    pub limit: usize,

    /// Step between block starts in bytes (default: block size in bytes).
    #[arg(long, default_value_t = 0)]
    pub step_bytes: usize,
}

#[derive(Args)]
pub struct OneArgs {
    /// Block as hex bytes (no 0x; e.g. "010203ff")
    #[arg(long)]
    pub hex: String,

    /// Block size in bits to interpret (<= bits in hex)
    #[arg(long, default_value_t = 128)]
    pub block_bits: usize,

    /// MOD
    #[arg(long, default_value_t = 4_294_967_291u64)]
    pub r#mod: u64,

    /// Base constant P
    #[arg(long, default_value = "0x243f6a8885a308d3")]
    pub p: String,

    /// Derivation mode: int | crc32 | decpairs
    #[arg(long, default_value = "int")]
    pub derive: String,
}

pub fn run(args: OrbExpArgs) -> anyhow::Result<()> {
    match args.cmd {
        OrbExpCmd::Blockscan(a) => cmd_blockscan(a),
        OrbExpCmd::One(a) => cmd_one(a),
    }
}

fn cmd_one(a: OneArgs) -> anyhow::Result<()> {
    let bytes = hex_to_bytes(&a.hex)?;
    let p = parse_u64_any(&a.p)?;
    let derive = DeriveMode::parse(&a.derive).map_err(|e| anyhow::anyhow!("{e}"))?;

    let (delta, step_a, step_c) = derive_steps(p, &bytes, a.block_bits, derive, a.r#mod)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let r = compute_first_meet(OrbParams {
        modn: a.r#mod,
        step_a,
        step_c,
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    println!("derive      = {:?}", derive);
    println!("block_bits  = {}", a.block_bits);
    println!("mod         = {}", a.r#mod);
    println!("p           = {}", p);
    println!("delta       = {}", delta);
    println!("step_a      = {}", step_a);
    println!("step_c      = {}", step_c);
    println!("d           = {}", r.d);
    println!("gcd         = {}", r.gcd);
    println!("t_first_meet= {} (bitlen={})", r.t_first_meet, bitlen_u64(r.t_first_meet));
    Ok(())
}

fn cmd_blockscan(a: BlockScanArgs) -> anyhow::Result<()> {
    let data = std::fs::read(&a.r#in)?;
    let p = parse_u64_any(&a.p)?;
    let derive = DeriveMode::parse(&a.derive).map_err(|e| anyhow::anyhow!("{e}"))?;

    let block_bytes = (a.block_bits + 7) / 8;
    if block_bytes == 0 {
        anyhow::bail!("block_bits must be > 0");
    }

    let step_bytes = if a.step_bytes == 0 { block_bytes } else { a.step_bytes };

    println!("idx,off,block_bits,derive,mod,p,delta,step_a,step_c,d,gcd,t_first_meet,t_bitlen");

    let mut idx: usize = 0;
    let mut off: usize = 0;

    use std::collections::HashMap;
    let mut meet_hist: HashMap<u64, usize> = HashMap::new();

    while off + block_bytes <= data.len() {
        if a.limit != 0 && idx >= a.limit {
            break;
        }

        let block = &data[off..off + block_bytes];

        let (delta, step_a, step_c) = derive_steps(p, block, a.block_bits, derive, a.r#mod)
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let r = compute_first_meet(OrbParams {
            modn: a.r#mod,
            step_a,
            step_c,
        })
        .map_err(|e| anyhow::anyhow!("{e}"))?;

        *meet_hist.entry(r.t_first_meet).or_insert(0) += 1;

        println!(
            "{},{},{},{:?},{},{},{},{},{},{},{},{},{}",
            idx,
            off,
            a.block_bits,
            derive,
            a.r#mod,
            p,
            delta,
            step_a,
            step_c,
            r.d,
            r.gcd,
            r.t_first_meet,
            bitlen_u64(r.t_first_meet)
        );

        idx += 1;
        off += step_bytes;
    }

    let mut collisions = 0usize;
    let mut unique = 0usize;
    for (_t, c) in meet_hist.iter() {
        unique += 1;
        if *c > 1 {
            collisions += *c - 1;
        }
    }

    eprintln!("blocks_scanned  = {}", idx);
    eprintln!("unique_meets    = {}", unique);
    eprintln!("collisions      = {}", collisions);
    Ok(())
}

fn parse_u64_any(s: &str) -> anyhow::Result<u64> {
    let t = s.trim();
    if let Some(hex) = t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")) {
        Ok(u64::from_str_radix(hex, 16)?)
    } else {
        Ok(t.parse::<u64>()?)
    }
}

fn hex_to_bytes(s: &str) -> anyhow::Result<Vec<u8>> {
    let t = s.trim();
    if t.len() % 2 != 0 {
        anyhow::bail!("hex length must be even");
    }
    let mut out = Vec::with_capacity(t.len() / 2);
    let bytes = t.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let hi = from_hex(bytes[i])?;
        let lo = from_hex(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn from_hex(b: u8) -> anyhow::Result<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(10 + (b - b'a')),
        b'A'..=b'F' => Ok(10 + (b - b'A')),
        _ => anyhow::bail!("invalid hex char"),
    }
}
