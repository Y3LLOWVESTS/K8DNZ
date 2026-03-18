// crates/k8dnz-cli/src/cmd/omega.rs

use anyhow::{anyhow, bail, Result};
use k8dnz_core::lane::{LaneOmega, LaneOmegaProg, OmegaProgram};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LaneName {
    Class,
    Kind,
    Case,
    Letter,
    Digit,
    Punct,
    Raw,
}

impl LaneName {
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "class" => Ok(Self::Class),
            "kind" => Ok(Self::Kind),
            "case" | "caseb" => Ok(Self::Case),
            "letter" => Ok(Self::Letter),
            "digit" => Ok(Self::Digit),
            "punct" | "punc" | "punctuation" => Ok(Self::Punct),
            "raw" => Ok(Self::Raw),
            _ => bail!("omega: unknown lane '{}'", s),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Class => "class",
            Self::Kind => "kind",
            Self::Case => "case",
            Self::Letter => "letter",
            Self::Digit => "digit",
            Self::Punct => "punct",
            Self::Raw => "raw",
        }
    }

    pub fn ordered() -> [Self; 7] {
        [
            Self::Class,
            Self::Kind,
            Self::Case,
            Self::Letter,
            Self::Digit,
            Self::Punct,
            Self::Raw,
        ]
    }

    pub fn set_prog(self, omega: &mut OmegaProgram, lane: LaneOmegaProg) {
        match self {
            Self::Class => omega.class = lane,
            Self::Kind => omega.kind = lane,
            Self::Case => omega.caseb = lane,
            Self::Letter => omega.letter = lane,
            Self::Digit => omega.digit = lane,
            Self::Punct => omega.punct = lane,
            Self::Raw => omega.raw = lane,
        }
    }

    pub fn get_prog(self, omega: &OmegaProgram) -> LaneOmegaProg {
        match self {
            Self::Class => omega.class.clone(),
            Self::Kind => omega.kind.clone(),
            Self::Case => omega.caseb.clone(),
            Self::Letter => omega.letter.clone(),
            Self::Digit => omega.digit.clone(),
            Self::Punct => omega.punct.clone(),
            Self::Raw => omega.raw.clone(),
        }
    }
}

/// Strict, deterministic Ω parser.
///
/// Supported formats (can mix per lane):
///
/// V1 lane (single segment):
///   "letter:skip=251,stride=1;kind:skip=113,stride=1"
///
/// V2 lane (2+ segments):
///   "letter:seg2=323:1|900:1"
///   "letter:seg3=323:1|900:1|1200:2"
///
/// Rules:
/// - segments separated by ';'
/// - each segment: "<lane>:<kv>[,<kv>...]" OR "<lane>:segN=a:b|c:d|..."
/// - kv keys supported: skip, stride
/// - omitted lanes keep default skip=0,stride=1 with 1 segment
pub fn parse_omega_spec(spec: &str) -> Result<OmegaProgram> {
    let s = spec.trim();
    if s.is_empty() {
        return Ok(OmegaProgram::default());
    }

    let mut omega = OmegaProgram::default();

    for seg in s.split(';') {
        let seg = seg.trim();
        if seg.is_empty() {
            continue;
        }

        let (lane_s, rest) = seg
            .split_once(':')
            .ok_or_else(|| anyhow!("omega: expected '<lane>:skip=..,stride=..' or '<lane>:segN=..' in segment '{}'", seg))?;

        let lane = LaneName::parse(lane_s)?;
        let rest = rest.trim();

        if let Some((n, v)) = parse_seg_directive(rest)? {
            let parts: Vec<&str> = v.split('|').map(|p| p.trim()).filter(|p| !p.is_empty()).collect();
            if parts.len() != n {
                bail!(
                    "omega: seg{} expects exactly {} segment pairs in '{}'",
                    n,
                    n,
                    seg
                );
            }

            let mut segs = Vec::with_capacity(n);
            for item in parts {
                let (a, b) = item
                    .split_once(':')
                    .ok_or_else(|| anyhow!("omega: seg item expects 'skip:stride', got '{}'", item))?;
                let skip = a.trim().parse::<u64>()?;
                let stride = b.trim().parse::<u64>()?;
                if stride == 0 {
                    bail!("omega: stride must be >= 1");
                }
                segs.push(LaneOmega { skip, stride });
            }

            let lp = LaneOmegaProg { segs };
            lp.validate().map_err(|e| anyhow!("{e}"))?;
            lane.set_prog(&mut omega, lp);
            continue;
        }

        let mut skip: Option<u64> = None;
        let mut stride: Option<u64> = None;

        for kv in rest.split(',') {
            let kv = kv.trim();
            if kv.is_empty() {
                continue;
            }
            let (k, v) = kv
                .split_once('=')
                .ok_or_else(|| anyhow!("omega: expected 'k=v' in segment '{}'", seg))?;

            match k.trim().to_ascii_lowercase().as_str() {
                "skip" => skip = Some(v.trim().parse::<u64>()?),
                "stride" => stride = Some(v.trim().parse::<u64>()?),
                _ => bail!("omega: unknown key '{}' in segment '{}'", k.trim(), seg),
            }
        }

        let mut lp = lane.get_prog(&omega);
        if lp.segs.is_empty() {
            lp = LaneOmegaProg::default();
        }

        let mut o = lp.segs[0];
        if let Some(v) = skip {
            o.skip = v;
        }
        if let Some(v) = stride {
            o.stride = v;
        }
        if o.stride == 0 {
            bail!("omega: stride must be >= 1 (lane {:?})", lane);
        }

        lp.segs = vec![o];
        lp.validate().map_err(|e| anyhow!("{e}"))?;
        lane.set_prog(&mut omega, lp);
    }

    omega.validate().map_err(|e| anyhow!("{e}"))?;
    Ok(omega)
}

pub fn omega_with_lane_single(
    base: &OmegaProgram,
    lane: LaneName,
    skip: u64,
    stride: u64,
) -> Result<OmegaProgram> {
    if stride == 0 {
        bail!("omega: stride must be >= 1");
    }
    let mut o = base.clone();
    lane.set_prog(&mut o, LaneOmegaProg::singleton(LaneOmega { skip, stride }));
    o.validate().map_err(|e| anyhow!("{e}"))?;
    Ok(o)
}

pub fn omega_with_lane(
    base: &OmegaProgram,
    lane: LaneName,
    skip: u64,
    stride: u64,
) -> Result<OmegaProgram> {
    omega_with_lane_single(base, lane, skip, stride)
}

pub fn omega_to_spec(omega: &OmegaProgram) -> String {
    let mut parts = Vec::new();

    for lane in LaneName::ordered() {
        let prog = lane.get_prog(omega);
        let lane_s = lane.as_str();
        if prog.segs.is_empty() {
            parts.push(format!("{}:skip=0,stride=1", lane_s));
            continue;
        }

        if prog.segs.len() == 1 {
            let seg = prog.segs[0];
            parts.push(format!(
                "{}:skip={},stride={}",
                lane_s, seg.skip, seg.stride
            ));
        } else {
            let joined = prog
                .segs
                .iter()
                .map(|seg| format!("{}:{}", seg.skip, seg.stride))
                .collect::<Vec<_>>()
                .join("|");
            parts.push(format!("{}:seg{}={}", lane_s, prog.segs.len(), joined));
        }
    }

    parts.join(";")
}

fn parse_seg_directive(s: &str) -> Result<Option<(usize, &str)>> {
    let (lhs, rhs) = match s.split_once('=') {
        Some(v) => v,
        None => return Ok(None),
    };

    let lhs = lhs.trim().to_ascii_lowercase();
    if !lhs.starts_with("seg") {
        return Ok(None);
    }

    let n: usize = lhs[3..].parse()?;
    if n == 0 {
        bail!("omega: seg count must be >= 1");
    }
    Ok(Some((n, rhs.trim())))
}
