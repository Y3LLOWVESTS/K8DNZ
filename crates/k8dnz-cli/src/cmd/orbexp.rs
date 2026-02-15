// crates/k8dnz-cli/src/cmd/orbexp.rs

use clap::{Args, Subcommand, ValueEnum};
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

    /// Split input into per-bucket lanes using meet-time buckets (for zstd/lane tests).
    /// Optionally also emits a single data stream + a tag stream (for "keep adjacency" experiments).
    Bandsplit(BandSplitArgs),

    /// Compute a single block (hex) meet-time (debug-friendly).
    One(OneArgs),
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum BucketFn {
    /// bucket = t_first_meet
    Tfirst,
    /// bucket = bitlen(t_first_meet)
    Tbitlen,
    /// bucket = (low32(gcd) << 32) ^ low32(t_first_meet)
    GcdTfirst,
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub enum TagFormat {
    /// 1 byte per tag (requires bucket_mod <= 256)
    Byte,
    /// Packed k-bit tags with a tiny TG1 header (requires bucket_mod <= 2^k)
    Packed,
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
pub struct BandSplitArgs {
    /// Input file path
    #[arg(long = "in")]
    pub r#in: String,

    /// Output prefix for lane files. Files will be: <prefix>.band_<id>.bin
    /// If --emit-tags is set, also writes: <prefix>.data.bin and <prefix>.tags.bin
    #[arg(long, default_value = "/tmp/k8dnz")]
    pub out_prefix: String,

    /// Block size in bits (e.g., 64 / 100 / 128)
    #[arg(long, default_value_t = 128)]
    pub block_bits: usize,

    /// Modular circle size (MOD).
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

    /// Bucket function for lane id derivation
    #[arg(long, value_enum, default_value_t = BucketFn::Tfirst)]
    pub bucket_fn: BucketFn,

    /// Shift bucket id right by N bits before applying bucket_mod (default: 0).
    /// Useful when buckets are large powers-of-two and low bits are always 0.
    #[arg(long, default_value_t = 0)]
    pub bucket_shift: u32,

    /// Reduce bucket id modulo N (0 = no reduction). Useful to force 4 timelines, etc.
    ///
    /// If --emit-tags AND tag_format=byte: bucket_mod must be <= 256.
    /// If --emit-tags AND tag_format=packed: bucket_mod must be <= 2^tag_bits.
    #[arg(long, default_value_t = 0)]
    pub bucket_mod: u64,

    /// Also write <out-prefix>.data.bin (concatenated blocks in scan order)
    /// and <out-prefix>.tags.bin (tag per block, derived from bucket id after shift/mod).
    #[arg(long, default_value_t = false)]
    pub emit_tags: bool,

    /// If set, tags are written as packed k-bit stream with a TG1 header.
    /// Output path is still <out-prefix>.tags.bin (but format differs).
    #[arg(long, default_value_t = false)]
    pub emit_tags_packed: bool,

    /// Tag format (byte or packed). If --emit-tags-packed is set, this is forced to packed.
    #[arg(long, value_enum, default_value_t = TagFormat::Byte)]
    pub tag_format: TagFormat,

    /// Bits per tag for packed tags (1..=8). Default: 2 (for mod4 timelines).
    #[arg(long, default_value_t = 2)]
    pub tag_bits: u8,

    /// Also print a per-lane summary to stderr at the end.
    #[arg(long, default_value_t = true)]
    pub summary: bool,
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
        OrbExpCmd::Bandsplit(a) => cmd_bandsplit(a),
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
    println!(
        "t_first_meet= {} (bitlen={})",
        r.t_first_meet,
        bitlen_u64(r.t_first_meet)
    );
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

    let step_bytes = if a.step_bytes == 0 {
        block_bytes
    } else {
        a.step_bytes
    };

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

// ---- packed tags format (tiny header + packed payload) ----
// Header:
//   magic: 4 bytes = "TG1\0"
//   bits_per_tag: u8
//   bucket_mod: u16 LE (0 means "unknown")
//   reserved: u8 (0)
//   tag_count: u64 LE
//   payload: packed bits (LSB-first within each byte)
const TAG_MAGIC: &[u8; 4] = b"TG1\0";

fn pack_tags(bits_per_tag: u8, tags: &[u8]) -> anyhow::Result<Vec<u8>> {
    if bits_per_tag == 0 || bits_per_tag > 8 {
        anyhow::bail!("bits_per_tag must be in 1..=8");
    }
    let total_bits = (tags.len() as usize) * (bits_per_tag as usize);
    let out_len = (total_bits + 7) / 8;
    let mut out = vec![0u8; out_len];

    let mut bitpos: usize = 0;
    for &t in tags.iter() {
        let v = (t as u16) & ((1u16 << bits_per_tag) - 1);
        for k in 0..(bits_per_tag as usize) {
            let b = ((v >> k) & 1) as u8;
            let byte_i = bitpos >> 3;
            let bit_i = bitpos & 7;
            out[byte_i] |= b << bit_i;
            bitpos += 1;
        }
    }

    Ok(out)
}

fn cmd_bandsplit(a: BandSplitArgs) -> anyhow::Result<()> {
    use std::collections::BTreeMap;
    use std::io::Write;

    let effective_tag_format = if a.emit_tags_packed {
        TagFormat::Packed
    } else {
        a.tag_format
    };

    if a.emit_tags {
        if a.bucket_mod == 0 {
            anyhow::bail!("--emit-tags requires --bucket-mod (set e.g. --bucket-mod 4)");
        }

        match effective_tag_format {
            TagFormat::Byte => {
                if a.bucket_mod > 256 {
                    anyhow::bail!(
                        "--emit-tags with --tag-format byte requires --bucket-mod <= 256"
                    );
                }
            }
            TagFormat::Packed => {
                if a.tag_bits == 0 || a.tag_bits > 8 {
                    anyhow::bail!("--tag-bits must be in 1..=8");
                }
                let cap = 1u64 << (a.tag_bits as u64);
                if a.bucket_mod > cap {
                    anyhow::bail!(
                        "--emit-tags with --tag-format packed requires --bucket-mod <= 2^tag_bits (bucket_mod={} tag_bits={} cap={})",
                        a.bucket_mod,
                        a.tag_bits,
                        cap
                    );
                }
            }
        }
    }

    let data = std::fs::read(&a.r#in)?;
    let p = parse_u64_any(&a.p)?;
    let derive = DeriveMode::parse(&a.derive).map_err(|e| anyhow::anyhow!("{e}"))?;

    let block_bytes = (a.block_bits + 7) / 8;
    if block_bytes == 0 {
        anyhow::bail!("block_bits must be > 0");
    }
    let step_bytes = if a.step_bytes == 0 {
        block_bytes
    } else {
        a.step_bytes
    };

    let mut idx: usize = 0;
    let mut off: usize = 0;

    let mut lanes: BTreeMap<u64, std::fs::File> = BTreeMap::new();
    let mut lane_bytes: BTreeMap<u64, u64> = BTreeMap::new();
    let mut lane_blocks: BTreeMap<u64, u64> = BTreeMap::new();

    let mut data_out: Option<std::fs::File> = None;

    // For tags:
    // - byte: stream directly to file
    // - packed: collect tags, then write header+packed once at end
    let mut tags_out: Option<std::fs::File> = None;
    let mut tags_vec: Vec<u8> = Vec::new();

    if a.emit_tags {
        let dp = format!("{}.data.bin", a.out_prefix);
        data_out = Some(std::fs::File::create(dp)?);

        let tp = format!("{}.tags.bin", a.out_prefix);
        tags_out = Some(std::fs::File::create(tp)?);
    }

    while off + block_bytes <= data.len() {
        if a.limit != 0 && idx >= a.limit {
            break;
        }

        let block = &data[off..off + block_bytes];

        let (_delta, step_a, step_c) = derive_steps(p, block, a.block_bits, derive, a.r#mod)
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        let r = compute_first_meet(OrbParams {
            modn: a.r#mod,
            step_a,
            step_c,
        })
        .map_err(|e| anyhow::anyhow!("{e}"))?;

        let mut bucket = bucket_id(a.bucket_fn, r.gcd, r.t_first_meet);

        if a.bucket_shift != 0 {
            bucket >>= a.bucket_shift;
        }

        if a.bucket_mod != 0 {
            bucket %= a.bucket_mod;
        }

        if let Some(f) = data_out.as_mut() {
            f.write_all(block)?;
        }

        if a.emit_tags {
            if bucket > u64::from(u8::MAX) && effective_tag_format == TagFormat::Byte {
                anyhow::bail!(
                    "bucket id {} does not fit in 1 byte; reduce with --bucket-mod <= 256 or use --tag-format packed",
                    bucket
                );
            }

            match effective_tag_format {
                TagFormat::Byte => {
                    if let Some(f) = tags_out.as_mut() {
                        f.write_all(&[bucket as u8])?;
                    }
                }
                TagFormat::Packed => {
                    tags_vec.push(bucket as u8);
                }
            }
        }

        let f = lanes.entry(bucket).or_insert_with(|| {
            let path = format!("{}.band_{}.bin", a.out_prefix, bucket);
            std::fs::File::create(path).expect("create lane file")
        });

        f.write_all(block)?;

        *lane_bytes.entry(bucket).or_insert(0) += block.len() as u64;
        *lane_blocks.entry(bucket).or_insert(0) += 1;

        idx += 1;
        off += step_bytes;
    }

    // Finalize packed tags if requested
    if a.emit_tags && effective_tag_format == TagFormat::Packed {
        let packed = pack_tags(a.tag_bits, &tags_vec)?;
        let tag_count = tags_vec.len() as u64;

        let mut header: Vec<u8> = Vec::with_capacity(4 + 1 + 2 + 1 + 8);
        header.extend_from_slice(TAG_MAGIC);
        header.push(a.tag_bits);
        header.extend_from_slice(&(a.bucket_mod as u16).to_le_bytes());
        header.push(0u8);
        header.extend_from_slice(&tag_count.to_le_bytes());

        if let Some(mut f) = tags_out.take() {
            f.write_all(&header)?;
            f.write_all(&packed)?;
        }
    }

    if a.summary {
        eprintln!("blocks_scanned = {}", idx);
        eprintln!("lanes_used     = {}", lanes.len());

        let mut total_bytes: u64 = 0;
        for (k, b) in lane_bytes.iter() {
            total_bytes += *b;
            let nblk = lane_blocks.get(k).copied().unwrap_or(0);
            eprintln!("lane {}: bytes={} blocks={}", k, b, nblk);
        }
        eprintln!("total_lane_bytes = {}", total_bytes);

        if a.emit_tags {
            match effective_tag_format {
                TagFormat::Byte => {
                    eprintln!(
                        "emitted: {}.data.bin ({} bytes), {}.tags.bin ({} bytes; 1 byte/tag)",
                        a.out_prefix,
                        (idx as u64) * (block_bytes as u64),
                        a.out_prefix,
                        idx
                    );
                }
                TagFormat::Packed => {
                    let packed_bits = (idx as u64) * (a.tag_bits as u64);
                    let packed_bytes = (packed_bits + 7) / 8;
                    let header_bytes = 4u64 + 1 + 2 + 1 + 8;
                    eprintln!(
                        "emitted: {}.data.bin ({} bytes), {}.tags.bin (~{} bytes payload + {} header; {} bits/tag)",
                        a.out_prefix,
                        (idx as u64) * (block_bytes as u64),
                        a.out_prefix,
                        packed_bytes,
                        header_bytes,
                        a.tag_bits
                    );
                }
            }
        }

        eprintln!("hint: zstd -3 each *.band_*.bin and compare sum vs full-file zstd");
    }

    Ok(())
}

fn bucket_id(bucket_fn: BucketFn, gcd: u64, t_first_meet: u64) -> u64 {
    match bucket_fn {
        BucketFn::Tfirst => t_first_meet,
        BucketFn::Tbitlen => bitlen_u64(t_first_meet) as u64,
        BucketFn::GcdTfirst => {
            let hi = (gcd & 0xFFFF_FFFF) << 32;
            let lo = t_first_meet & 0xFFFF_FFFF;
            hi ^ lo
        }
    }
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
