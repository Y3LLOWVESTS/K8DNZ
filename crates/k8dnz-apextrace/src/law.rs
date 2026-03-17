#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeState {
    pub q: u8,
    pub u: u64,
}

pub const BRANCH_ROOT: u8 = 0;
pub const BRANCH_LEFT: u8 = 1;
pub const BRANCH_RIGHT: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraceNode {
    pub step: u16,
    pub row: u16,
    pub k: u16,
    pub q: u8,
    pub u: u64,
    pub branch: u8,
}

pub fn root_state(root_quadrant: u8, root_seed: u64) -> NodeState {
    NodeState {
        q: root_quadrant & 0b11,
        u: root_seed,
    }
}

pub fn descend(mut state: NodeState, depth: u16, leaf_index: u64, recipe_seed: u64) -> NodeState {
    let mut level = 0u16;
    while level < depth {
        let bit_pos = u32::from(depth - 1 - level);
        let bit = ((leaf_index >> bit_pos) & 1) as u8;

        state = if bit == 0 {
            step_left(state, level, recipe_seed)
        } else {
            step_right(state, level, recipe_seed)
        };

        level += 1;
    }

    state
}

pub fn trace_leaf(
    root_quadrant: u8,
    root_seed: u64,
    depth: u16,
    leaf_index: u64,
    recipe_seed: u64,
) -> Vec<TraceNode> {
    let mut out = Vec::with_capacity(depth as usize + 1);
    let mut state = root_state(root_quadrant, root_seed);
    let mut k = 0u16;

    out.push(TraceNode {
        step: 0,
        row: 0,
        k,
        q: state.q,
        u: state.u,
        branch: BRANCH_ROOT,
    });

    let mut level = 0u16;
    while level < depth {
        let bit_pos = u32::from(depth - 1 - level);
        let bit = ((leaf_index >> bit_pos) & 1) as u8;

        let branch = if bit == 0 {
            state = step_left(state, level, recipe_seed);
            BRANCH_LEFT
        } else {
            state = step_right(state, level, recipe_seed);
            k = k.saturating_add(1);
            BRANCH_RIGHT
        };

        let row = level.saturating_add(1);
        out.push(TraceNode {
            step: row,
            row,
            k,
            q: state.q,
            u: state.u,
            branch,
        });

        level += 1;
    }

    out
}

pub fn emit_quat(state: NodeState) -> u8 {
    1 + (((state.q as u64) + (state.u & 0b11)) % 4) as u8
}

fn step_left(state: NodeState, level: u16, recipe_seed: u64) -> NodeState {
    let bump = 1u8.wrapping_add((state.u & 1) as u8);
    let q = (state.q.wrapping_add(bump)) & 0b11;
    let u = mix(state.u, level, 0, recipe_seed);
    NodeState { q, u }
}

fn step_right(state: NodeState, level: u16, recipe_seed: u64) -> NodeState {
    let bump = 3u8.wrapping_add(((state.u >> 1) & 1) as u8);
    let q = (state.q.wrapping_add(bump)) & 0b11;
    let u = mix(state.u, level, 1, recipe_seed);
    NodeState { q, u }
}

fn mix(u: u64, level: u16, side: u8, recipe_seed: u64) -> u64 {
    const C1: u64 = 0x9E37_79B9_7F4A_7C15;
    const C2: u64 = 0xBF58_476D_1CE4_E5B9;
    const C3: u64 = 0x94D0_49BB_1331_11EB;

    let level64 = u64::from(level);
    let rotate = ((u32::from(level) * 7) + (u32::from(side) * 13) + 11) & 63;

    let mut x = u ^ recipe_seed.rotate_left((u32::from(level) * 5 + 17) & 63);
    x ^= (level64 << 32) ^ (level64 << 1);
    x ^= if side == 0 { C1 } else { C2 };
    x = x.wrapping_mul(C1 ^ (level64.wrapping_mul(C3)));
    x ^= x >> 29;
    x = x.rotate_left(rotate);
    x = x.wrapping_mul(C2 ^ recipe_seed.rotate_right((u32::from(level) * 3 + 9) & 63));
    x ^ (x >> 31)
}