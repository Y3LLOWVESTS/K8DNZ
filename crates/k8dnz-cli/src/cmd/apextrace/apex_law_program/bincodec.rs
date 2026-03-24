use anyhow::{bail, Context, Result};
use std::io::{Cursor, Read};

use super::types::{
    LawProgramArtifact, ProgramFile, ProgramOverride, ProgramSummary, ProgramWindow, ReplayConfig,
};

const ARTIFACT_MAGIC: &[u8; 4] = b"AKLP";
const ARTIFACT_VERSION: u8 = 1;

impl LawProgramArtifact {
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let mut w = BinWriter::default();
        w.bytes(ARTIFACT_MAGIC);
        w.u8(ARTIFACT_VERSION);

        self.config.encode(&mut w);
        self.summary.encode(&mut w);

        w.uvar(self.files.len() as u64);
        for row in &self.files {
            row.encode(&mut w);
        }

        w.uvar(self.windows.len() as u64);
        for row in &self.windows {
            row.encode(&mut w);
        }

        w.uvar(self.overrides.len() as u64);
        for row in &self.overrides {
            row.encode(&mut w);
        }

        Ok(w.finish())
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let mut r = BinReader::new(bytes);

        let magic = r.fixed_bytes(4)?;
        if magic.as_slice() != ARTIFACT_MAGIC {
            bail!("bad artifact magic");
        }

        let version = r.u8()?;
        if version != ARTIFACT_VERSION {
            bail!("unsupported artifact version {}", version);
        }

        let config = ReplayConfig::decode(&mut r)?;
        let summary = ProgramSummary::decode(&mut r)?;

        let file_len = r.uvar()? as usize;
        let mut files = Vec::with_capacity(file_len);
        for _ in 0..file_len {
            files.push(ProgramFile::decode(&mut r)?);
        }

        let window_len = r.uvar()? as usize;
        let mut windows = Vec::with_capacity(window_len);
        for _ in 0..window_len {
            windows.push(ProgramWindow::decode(&mut r)?);
        }

        let override_len = r.uvar()? as usize;
        let mut overrides = Vec::with_capacity(override_len);
        for _ in 0..override_len {
            overrides.push(ProgramOverride::decode(&mut r)?);
        }

        if !r.is_eof() {
            bail!("trailing bytes after artifact decode");
        }

        Ok(Self {
            config,
            summary,
            files,
            windows,
            overrides,
        })
    }
}

#[derive(Default)]
struct BinWriter {
    buf: Vec<u8>,
}

impl BinWriter {
    fn finish(self) -> Vec<u8> {
        self.buf
    }

    fn bytes(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    fn bool(&mut self, v: bool) {
        self.u8(if v { 1 } else { 0 });
    }

    fn uvar(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push(((v as u8) & 0x7F) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }

    fn ivar(&mut self, v: i64) {
        let zigzag = ((v << 1) ^ (v >> 63)) as u64;
        self.uvar(zigzag);
    }

    fn string(&mut self, s: &str) {
        self.uvar(s.len() as u64);
        self.bytes(s.as_bytes());
    }

    fn opt_u64(&mut self, v: Option<u64>) {
        match v {
            Some(v) => {
                self.bool(true);
                self.uvar(v);
            }
            None => self.bool(false),
        }
    }

    fn opt_string(&mut self, v: &Option<String>) {
        match v {
            Some(v) => {
                self.bool(true);
                self.string(v);
            }
            None => self.bool(false),
        }
    }
}

struct BinReader<'a> {
    cur: Cursor<&'a [u8]>,
}

impl<'a> BinReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            cur: Cursor::new(bytes),
        }
    }

    fn is_eof(&self) -> bool {
        self.cur.position() as usize == self.cur.get_ref().len()
    }

    fn u8(&mut self) -> Result<u8> {
        let mut b = [0u8; 1];
        self.cur.read_exact(&mut b).context("read u8")?;
        Ok(b[0])
    }

    fn bool(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            v => bail!("invalid bool byte {}", v),
        }
    }

    fn uvar(&mut self) -> Result<u64> {
        let mut shift = 0u32;
        let mut out = 0u64;

        loop {
            let b = self.u8()?;
            out |= ((b & 0x7F) as u64) << shift;
            if (b & 0x80) == 0 {
                return Ok(out);
            }
            shift += 7;
            if shift >= 64 {
                bail!("uvar too large");
            }
        }
    }

    fn ivar(&mut self) -> Result<i64> {
        let u = self.uvar()?;
        Ok(((u >> 1) as i64) ^ (-((u & 1) as i64)))
    }

    fn fixed_bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        let mut out = vec![0u8; len];
        self.cur
            .read_exact(&mut out)
            .with_context(|| format!("read {} bytes", len))?;
        Ok(out)
    }

    fn string(&mut self) -> Result<String> {
        let len = self.uvar()? as usize;
        let bytes = self.fixed_bytes(len)?;
        String::from_utf8(bytes).context("decode utf8 string")
    }

    fn opt_u64(&mut self) -> Result<Option<u64>> {
        if self.bool()? {
            Ok(Some(self.uvar()?))
        } else {
            Ok(None)
        }
    }

    fn opt_string(&mut self) -> Result<Option<String>> {
        if self.bool()? {
            Ok(Some(self.string()?))
        } else {
            Ok(None)
        }
    }
}

impl ReplayConfig {
    fn encode(&self, w: &mut BinWriter) {
        w.string(&self.recipe);
        w.uvar(self.inputs.len() as u64);
        for s in &self.inputs {
            w.string(s);
        }
        w.uvar(self.max_ticks);
        w.uvar(self.window_bytes as u64);
        w.uvar(self.step_bytes as u64);
        w.uvar(self.max_windows as u64);
        w.uvar(self.seed_from);
        w.uvar(self.seed_count);
        w.uvar(self.seed_step);
        w.uvar(self.recipe_seed);
        w.string(&self.chunk_sweep);
        w.string(&self.chunk_search_objective);
        w.uvar(self.chunk_raw_slack);
        w.uvar(self.map_max_depth as u64);
        w.uvar(self.map_depth_shift as u64);
        w.string(&self.boundary_band_sweep);
        w.uvar(self.boundary_delta as u64);
        w.string(&self.field_margin_sweep);
        w.uvar(self.newline_margin_add);
        w.uvar(self.space_to_newline_margin_add);
        w.uvar(self.newline_share_ppm_min as u64);
        w.uvar(self.newline_override_budget as u64);
        w.string(&self.newline_demote_margin_sweep);
        w.uvar(self.newline_demote_keep_ppm_min as u64);
        w.uvar(self.newline_demote_keep_min as u64);
        w.bool(self.newline_only_from_spacelike);
        w.uvar(self.merge_gap_bytes as u64);
        w.bool(self.allow_overlap_scout);
        w.opt_u64(self.freeze_boundary_band.map(|v| v as u64));
        w.opt_u64(self.freeze_field_margin);
        w.opt_u64(self.freeze_newline_demote_margin);
        w.string(&self.local_chunk_sweep);
        w.opt_string(&self.local_chunk_search_objective);
        w.opt_u64(self.local_chunk_raw_slack);
        w.opt_u64(self.default_local_chunk_bytes_arg.map(|v| v as u64));
        w.bool(self.tune_default_body);
        w.opt_string(&self.default_body_chunk_sweep);
        w.string(&self.body_select_objective);
        w.bool(self.emit_body_scoreboard);
        w.uvar(self.min_override_gain_exact as u64);
        w.uvar(self.exact_subset_limit as u64);
        w.opt_string(&self.global_law_id_arg);
    }

    fn decode(r: &mut BinReader<'_>) -> Result<Self> {
        let recipe = r.string()?;
        let inputs_len = r.uvar()? as usize;
        let mut inputs = Vec::with_capacity(inputs_len);
        for _ in 0..inputs_len {
            inputs.push(r.string()?);
        }

        Ok(Self {
            recipe,
            inputs,
            max_ticks: r.uvar()?,
            window_bytes: r.uvar()? as usize,
            step_bytes: r.uvar()? as usize,
            max_windows: r.uvar()? as usize,
            seed_from: r.uvar()?,
            seed_count: r.uvar()?,
            seed_step: r.uvar()?,
            recipe_seed: r.uvar()?,
            chunk_sweep: r.string()?,
            chunk_search_objective: r.string()?,
            chunk_raw_slack: r.uvar()?,
            map_max_depth: r.uvar()? as u8,
            map_depth_shift: r.uvar()? as u8,
            boundary_band_sweep: r.string()?,
            boundary_delta: r.uvar()? as usize,
            field_margin_sweep: r.string()?,
            newline_margin_add: r.uvar()?,
            space_to_newline_margin_add: r.uvar()?,
            newline_share_ppm_min: r.uvar()? as u32,
            newline_override_budget: r.uvar()? as usize,
            newline_demote_margin_sweep: r.string()?,
            newline_demote_keep_ppm_min: r.uvar()? as u32,
            newline_demote_keep_min: r.uvar()? as usize,
            newline_only_from_spacelike: r.bool()?,
            merge_gap_bytes: r.uvar()? as usize,
            allow_overlap_scout: r.bool()?,
            freeze_boundary_band: r.opt_u64()?.map(|v| v as usize),
            freeze_field_margin: r.opt_u64()?,
            freeze_newline_demote_margin: r.opt_u64()?,
            local_chunk_sweep: r.string()?,
            local_chunk_search_objective: r.opt_string()?,
            local_chunk_raw_slack: r.opt_u64()?,
            default_local_chunk_bytes_arg: r.opt_u64()?.map(|v| v as usize),
            tune_default_body: r.bool()?,
            default_body_chunk_sweep: r.opt_string()?,
            body_select_objective: r.string()?,
            emit_body_scoreboard: r.bool()?,
            min_override_gain_exact: r.uvar()? as usize,
            exact_subset_limit: r.uvar()? as usize,
            global_law_id_arg: r.opt_string()?,
        })
    }
}

impl ProgramSummary {
    fn encode(&self, w: &mut BinWriter) {
        w.string(&self.recipe);
        w.uvar(self.file_count as u64);
        w.uvar(self.honest_file_count as u64);
        w.uvar(self.union_law_count as u64);
        w.string(&self.target_global_law_id);
        w.uvar(self.target_global_law_path_hits as u64);
        w.uvar(self.target_global_law_file_count as u64);
        w.uvar(self.target_global_law_total_window_count as u64);
        w.uvar(self.target_global_law_total_segment_count as u64);
        w.uvar(self.target_global_law_total_covered_bytes as u64);
        w.string(&self.target_global_law_dominant_knob_signature);
        w.uvar(self.eval_boundary_band as u64);
        w.uvar(self.eval_field_margin);
        w.uvar(self.eval_newline_demote_margin);
        w.string(&self.eval_chunk_search_objective);
        w.uvar(self.eval_chunk_raw_slack);
        w.string(&self.eval_chunk_candidates);
        w.uvar(self.eval_chunk_candidate_count as u64);
        w.uvar(self.default_local_chunk_bytes as u64);
        w.uvar(self.default_local_chunk_window_wins as u64);
        w.ivar(self.searched_total_piecewise_payload_exact);
        w.ivar(self.projected_default_total_piecewise_payload_exact);
        w.ivar(self.delta_default_total_piecewise_payload_exact);
        w.ivar(self.projected_unpriced_best_mix_total_piecewise_payload_exact);
        w.ivar(self.delta_unpriced_best_mix_total_piecewise_payload_exact);
        w.ivar(self.selected_total_piecewise_payload_exact);
        w.ivar(self.delta_selected_total_piecewise_payload_exact);
        w.uvar(self.target_window_count as u64);
        w.uvar(self.searched_target_window_payload_exact as u64);
        w.uvar(self.default_target_window_payload_exact as u64);
        w.uvar(self.best_mix_target_window_payload_exact as u64);
        w.uvar(self.selected_target_window_payload_exact as u64);
        w.ivar(self.delta_selected_target_window_payload_exact);
        w.string(&self.override_path_mode);
        w.uvar(self.override_path_bytes_exact as u64);
        w.uvar(self.selected_override_window_count as u64);
        w.uvar(self.improved_target_window_count as u64);
        w.uvar(self.equal_target_window_count as u64);
        w.uvar(self.worsened_target_window_count as u64);
    }

    fn decode(r: &mut BinReader<'_>) -> Result<Self> {
        Ok(Self {
            recipe: r.string()?,
            file_count: r.uvar()? as usize,
            honest_file_count: r.uvar()? as usize,
            union_law_count: r.uvar()? as usize,
            target_global_law_id: r.string()?,
            target_global_law_path_hits: r.uvar()? as usize,
            target_global_law_file_count: r.uvar()? as usize,
            target_global_law_total_window_count: r.uvar()? as usize,
            target_global_law_total_segment_count: r.uvar()? as usize,
            target_global_law_total_covered_bytes: r.uvar()? as usize,
            target_global_law_dominant_knob_signature: r.string()?,
            eval_boundary_band: r.uvar()? as usize,
            eval_field_margin: r.uvar()?,
            eval_newline_demote_margin: r.uvar()?,
            eval_chunk_search_objective: r.string()?,
            eval_chunk_raw_slack: r.uvar()?,
            eval_chunk_candidates: r.string()?,
            eval_chunk_candidate_count: r.uvar()? as usize,
            default_local_chunk_bytes: r.uvar()? as usize,
            default_local_chunk_window_wins: r.uvar()? as usize,
            searched_total_piecewise_payload_exact: r.ivar()?,
            projected_default_total_piecewise_payload_exact: r.ivar()?,
            delta_default_total_piecewise_payload_exact: r.ivar()?,
            projected_unpriced_best_mix_total_piecewise_payload_exact: r.ivar()?,
            delta_unpriced_best_mix_total_piecewise_payload_exact: r.ivar()?,
            selected_total_piecewise_payload_exact: r.ivar()?,
            delta_selected_total_piecewise_payload_exact: r.ivar()?,
            target_window_count: r.uvar()? as usize,
            searched_target_window_payload_exact: r.uvar()? as usize,
            default_target_window_payload_exact: r.uvar()? as usize,
            best_mix_target_window_payload_exact: r.uvar()? as usize,
            selected_target_window_payload_exact: r.uvar()? as usize,
            delta_selected_target_window_payload_exact: r.ivar()?,
            override_path_mode: r.string()?,
            override_path_bytes_exact: r.uvar()? as usize,
            selected_override_window_count: r.uvar()? as usize,
            improved_target_window_count: r.uvar()? as usize,
            equal_target_window_count: r.uvar()? as usize,
            worsened_target_window_count: r.uvar()? as usize,
        })
    }
}

impl ProgramFile {
    fn encode(&self, w: &mut BinWriter) {
        w.string(&self.input);
        w.ivar(self.searched_total_piecewise_payload_exact);
        w.ivar(self.projected_default_total_piecewise_payload_exact);
        w.ivar(self.projected_unpriced_best_mix_total_piecewise_payload_exact);
        w.ivar(self.selected_total_piecewise_payload_exact);
        w.uvar(self.target_window_count as u64);
        w.string(&self.override_path_mode);
        w.uvar(self.override_path_bytes_exact as u64);
        w.uvar(self.selected_override_window_count as u64);
    }

    fn decode(r: &mut BinReader<'_>) -> Result<Self> {
        Ok(Self {
            input: r.string()?,
            searched_total_piecewise_payload_exact: r.ivar()?,
            projected_default_total_piecewise_payload_exact: r.ivar()?,
            projected_unpriced_best_mix_total_piecewise_payload_exact: r.ivar()?,
            selected_total_piecewise_payload_exact: r.ivar()?,
            target_window_count: r.uvar()? as usize,
            override_path_mode: r.string()?,
            override_path_bytes_exact: r.uvar()? as usize,
            selected_override_window_count: r.uvar()? as usize,
        })
    }
}

impl ProgramWindow {
    fn encode(&self, w: &mut BinWriter) {
        w.uvar(self.input_index as u64);
        w.string(&self.input);
        w.uvar(self.window_idx as u64);
        w.uvar(self.target_ordinal as u64);
        w.uvar(self.start as u64);
        w.uvar(self.end as u64);
        w.uvar(self.span_bytes as u64);
        w.uvar(self.searched_payload_exact as u64);
        w.uvar(self.default_payload_exact as u64);
        w.uvar(self.best_payload_exact as u64);
        w.uvar(self.selected_payload_exact as u64);
        w.uvar(self.searched_chunk_bytes as u64);
        w.uvar(self.best_chunk_bytes as u64);
        w.uvar(self.selected_chunk_bytes as u64);
        w.ivar(self.selected_gain_exact);
    }

    fn decode(r: &mut BinReader<'_>) -> Result<Self> {
        Ok(Self {
            input_index: r.uvar()? as usize,
            input: r.string()?,
            window_idx: r.uvar()? as usize,
            target_ordinal: r.uvar()? as usize,
            start: r.uvar()? as usize,
            end: r.uvar()? as usize,
            span_bytes: r.uvar()? as usize,
            searched_payload_exact: r.uvar()? as usize,
            default_payload_exact: r.uvar()? as usize,
            best_payload_exact: r.uvar()? as usize,
            selected_payload_exact: r.uvar()? as usize,
            searched_chunk_bytes: r.uvar()? as usize,
            best_chunk_bytes: r.uvar()? as usize,
            selected_chunk_bytes: r.uvar()? as usize,
            selected_gain_exact: r.ivar()?,
        })
    }
}

impl ProgramOverride {
    fn encode(&self, w: &mut BinWriter) {
        w.uvar(self.input_index as u64);
        w.string(&self.input);
        w.uvar(self.window_idx as u64);
        w.uvar(self.target_ordinal as u64);
        w.uvar(self.best_chunk_bytes as u64);
        w.uvar(self.default_payload_exact as u64);
        w.uvar(self.best_payload_exact as u64);
        w.uvar(self.gain_exact as u64);
    }

    fn decode(r: &mut BinReader<'_>) -> Result<Self> {
        Ok(Self {
            input_index: r.uvar()? as usize,
            input: r.string()?,
            window_idx: r.uvar()? as usize,
            target_ordinal: r.uvar()? as usize,
            best_chunk_bytes: r.uvar()? as usize,
            default_payload_exact: r.uvar()? as usize,
            best_payload_exact: r.uvar()? as usize,
            gain_exact: r.uvar()? as usize,
        })
    }
}