// crates/k8dnz-core/src/signal/token.rs

/// Back-compat alias: packed (a<<4)|b byte from a PairToken.
/// lib.rs re-exports this, so it must exist.
pub type PackedByte = u8;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct PairToken {
    pub a: u8,
    pub b: u8,
}

impl PairToken {
    /// Pack N=16 pair into one byte: (a<<4)|b
    #[inline]
    pub fn pack_byte(self) -> PackedByte {
        ((self.a & 0x0F) << 4) | (self.b & 0x0F)
    }

    /// Back-compat name used in older code.
    #[inline]
    pub fn pack_n16(self) -> PackedByte {
        self.pack_byte()
    }

    #[inline]
    pub fn unpack_byte(x: PackedByte) -> Self {
        Self {
            a: (x >> 4) & 0x0F,
            b: x & 0x0F,
        }
    }

    /// Deterministic “color pair” view of this token.
    /// MVP palette-based mapping; later we’ll swap in additive/coupled cone laws.
    #[inline]
    pub fn to_rgb_pair(self) -> RgbPairToken {
        RgbPairToken {
            a: palette16(self.a & 0x0F),
            c: palette16(self.b & 0x0F),
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Rgb {
    pub r: u8,
    pub g: u8,
    pub b: u8,
}

impl Rgb {
    pub const fn new(r: u8, g: u8, b: u8) -> Self {
        Self { r, g, b }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RgbPairToken {
    /// Dot A color
    pub a: Rgb,
    /// Dot C color (named “c” to match the vision: A + C are the emitters)
    pub c: Rgb,
}

impl RgbPairToken {
    /// 6 bytes: A.rgb then C.rgb
    #[inline]
    pub fn to_bytes(self) -> [u8; 6] {
        [self.a.r, self.a.g, self.a.b, self.c.r, self.c.g, self.c.b]
    }
}

/// A compact, deterministic 16-color palette that “reads” like an orderly spectrum.
/// This is a deliberate MVP: stable, repeatable, and non-gray-heavy.
///
/// Later we’ll replace/augment this with:
/// - additive cone RGB (base colors + ramp + field modulation)
/// - coupled-adder DNA model (shared drift + differential symbols)
pub fn palette16(n: u8) -> Rgb {
    const P: [Rgb; 16] = [
        Rgb::new(255,  60,  60), // 0 red
        Rgb::new(255, 120,  60), // 1 red-orange
        Rgb::new(255, 180,  60), // 2 orange
        Rgb::new(255, 230,  60), // 3 yellow
        Rgb::new(200, 255,  60), // 4 yellow-green
        Rgb::new(120, 255,  60), // 5 green
        Rgb::new( 60, 255, 120), // 6 green-cyan
        Rgb::new( 60, 255, 200), // 7 cyan
        Rgb::new( 60, 230, 255), // 8 cyan-sky
        Rgb::new( 60, 180, 255), // 9 sky-blue
        Rgb::new( 60, 120, 255), // A blue
        Rgb::new(120,  60, 255), // B indigo
        Rgb::new(180,  60, 255), // C purple
        Rgb::new(230,  60, 255), // D magenta
        Rgb::new(255,  60, 200), // E hot pink
        Rgb::new(255,  60, 120), // F pink-red
    ];

    P[(n & 0x0F) as usize]
}
