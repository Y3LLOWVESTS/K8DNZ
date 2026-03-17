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
/// V2 lane (2 segments, frustum climb MVP):
///   "letter:seg2=323:1|900:1"
///   where seg2=a:b|c:d means:
///     segment0: skip=a, stride=b
///     segment1: skip=c, stride=d
///
/// Rules:
/// - segments separated by ';'
/// - each segment: "<lane>:<kv>[,<kv>...]" OR "<lane>:seg2=a:b|c:d"
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
            .ok_or_else(|| anyhow!("omega: expected '<lane>:skip=..,stride=..' or '<lane>:seg2=..' in segment '{}'", seg))?;

        let lane = LaneName::parse(lane_s)?;
        let rest = rest.trim();

        // seg2=...
        if let Some(v) = rest.strip_prefix("seg2=") {
            let v = v.trim();
            // format: a:b|c:d
            let (s0, s1) = v
                .split_once('|')
                .ok_or_else(|| anyhow!("omega: seg2 expects 'a:b|c:d' in segment '{}'", seg))?;

            fn parse_pair(s: &str) -> Result<LaneOmega> {
                let s = s.trim();
                let (a, b) = s
                    .split_once(':')
                    .ok_or_else(|| anyhow!("omega: seg2 item expects 'skip:stride', got '{}'", s))?;
                let skip = a.trim().parse::<u64>()?;
                let stride = b.trim().parse::<u64>()?;
                if stride == 0 {
                    bail!("omega: stride must be >= 1");
                }
                Ok(LaneOmega { skip, stride })
            }

            let o0 = parse_pair(s0)?;
            let o1 = parse_pair(s1)?;

            let lp = LaneOmegaProg { segs: vec![o0, o1] };
            lp.validate().map_err(|e| anyhow!("{e}"))?;
            lane.set_prog(&mut omega, lp);
            continue;
        }

        // v1: skip/stride kvs apply to lane singleton program
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

            let k = k.trim().to_ascii_lowercase();
            let v = v.trim();

            match k.as_str() {
                "skip" => skip = Some(v.parse::<u64>()?),
                "stride" => stride = Some(v.parse::<u64>()?),
                _ => bail!("omega: unknown key '{}' in segment '{}'", k, seg),
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

/// Convenience: override exactly one lane with a singleton (used by sweep).
pub fn omega_with_lane_single(base: &OmegaProgram, lane: LaneName, skip: u64, stride: u64) -> Result<OmegaProgram> {
    if stride == 0 {
        bail!("omega: stride must be >= 1");
    }
    let mut o = base.clone();
    lane.set_prog(&mut o, LaneOmegaProg::singleton(LaneOmega { skip, stride }));
    o.validate().map_err(|e| anyhow!("{e}"))?;
    Ok(o)
}
/// Back-compat shim: old sweep code expects `omega_with_lane(...)`.
/// New world uses OmegaProgram; this just forwards.
pub fn omega_with_lane(base: &OmegaProgram, lane: LaneName, skip: u64, stride: u64) -> Result<OmegaProgram> {
    omega_with_lane_single(base, lane, skip, stride)
}
