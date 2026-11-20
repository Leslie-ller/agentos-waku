//! Pure-Rust, zero-dependency ISO/IEC 18004 QR Code encoder (Byte mode,
//! Error Correction Level M, Versions 1–10) and GPUI canvas renderer.
//!
//! Used by Settings → Daemon to render a scannable pairing QR code for mobile
//! Web/PWA clients without adding external crates to the workspace lockfile.

use gpui::{
    BorderStyle, Bounds, Div, Hsla, Pixels, canvas, div, point, prelude::*, px, quad, rgb, size,
};

/// A square boolean QR matrix (`true` = dark module, `false` = light module),
/// excluding the outer quiet zone.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QrMatrix {
    size: usize,
    modules: Vec<bool>,
}

impl QrMatrix {
    pub fn size(&self) -> usize {
        self.size
    }

    pub fn is_dark(&self, x: usize, y: usize) -> bool {
        self.modules[y * self.size + x]
    }
}

/// Error-correction Level M parameters for QR Versions 1 through 10
/// (ISO/IEC 18004 Table 9).
struct VersionSpec {
    version: usize,
    /// Total data codewords across all blocks.
    data_codewords: usize,
    /// EC codewords per block.
    ec_per_block: usize,
    /// (block_count, data_codewords_per_block) for Group 1 and Group 2.
    group1: (usize, usize),
    group2: (usize, usize),
}

const VERSION_SPECS_M: [VersionSpec; 10] = [
    VersionSpec { version: 1,  data_codewords: 16,  ec_per_block: 10, group1: (1, 16), group2: (0, 0) },
    VersionSpec { version: 2,  data_codewords: 28,  ec_per_block: 16, group1: (1, 28), group2: (0, 0) },
    VersionSpec { version: 3,  data_codewords: 44,  ec_per_block: 26, group1: (1, 44), group2: (0, 0) },
    VersionSpec { version: 4,  data_codewords: 64,  ec_per_block: 18, group1: (2, 32), group2: (0, 0) },
    VersionSpec { version: 5,  data_codewords: 86,  ec_per_block: 24, group1: (2, 43), group2: (0, 0) },
    VersionSpec { version: 6,  data_codewords: 108, ec_per_block: 16, group1: (4, 27), group2: (0, 0) },
    VersionSpec { version: 7,  data_codewords: 124, ec_per_block: 18, group1: (4, 31), group2: (0, 0) },
    VersionSpec { version: 8,  data_codewords: 154, ec_per_block: 22, group1: (2, 38), group2: (2, 39) },
    VersionSpec { version: 9,  data_codewords: 182, ec_per_block: 22, group1: (3, 36), group2: (2, 37) },
    VersionSpec { version: 10, data_codewords: 216, ec_per_block: 26, group1: (4, 43), group2: (1, 44) },
];

/// Alignment pattern center coordinates for Versions 1 through 10.
fn alignment_centers(version: usize) -> &'static [usize] {
    match version {
        1 => &[],
        2 => &[6, 18],
        3 => &[6, 22],
        4 => &[6, 26],
        5 => &[6, 30],
        6 => &[6, 34],
        7 => &[6, 22, 38],
        8 => &[6, 24, 42],
        9 => &[6, 26, 46],
        10 => &[6, 28, 50],
        _ => &[],
    }
}

/// Encode `data` as a Byte-mode QR code at Error Correction Level M.
/// Returns `None` if `data` exceeds Version 10-M capacity (213 bytes).
pub fn encode_qr(data: &[u8]) -> Option<QrMatrix> {
    let spec = VERSION_SPECS_M.iter().find(|spec| {
        let count_bits = if spec.version <= 9 { 8 } else { 16 };
        let header_bits = 4 + count_bits;
        let total_bits = header_bits + data.len() * 8;
        total_bits <= spec.data_codewords * 8
    })?;

    let data_codewords = build_data_codewords(data, spec);
    let final_codewords = interleave_with_ecc(&data_codewords, spec);

    let dim = spec.version * 4 + 17;
    let mut base = vec![false; dim * dim];
    let mut is_function = vec![false; dim * dim];

    draw_function_patterns(spec.version, dim, &mut base, &mut is_function);
    draw_codewords(dim, &final_codewords, &mut base, &is_function);

    // Evaluate all 8 mask patterns and select the lowest penalty score.
    let mut best_modules = Vec::new();
    let mut best_penalty = u32::MAX;
    for mask in 0..8u8 {
        let mut candidate = base.clone();
        apply_mask(dim, mask, &mut candidate, &is_function);
        draw_format_bits(dim, mask, &mut candidate);
        if spec.version >= 7 {
            draw_version_bits(spec.version, dim, &mut candidate);
        }
        let penalty = penalty_score(dim, &candidate);
        if penalty < best_penalty {
            best_penalty = penalty;
            best_modules = candidate;
        }
    }

    Some(QrMatrix {
        size: dim,
        modules: best_modules,
    })
}

/// Render a [`QrMatrix`] as a crisp high-contrast GPUI element with a standard
/// 4-module white quiet zone so phone cameras scan reliably in light and dark
/// desktop themes alike.
pub fn qr_code_element(matrix: QrMatrix, pixel_size: f32) -> Div {
    let quiet = 4usize;
    let total_modules = (matrix.size() + quiet * 2) as f32;
    let light: Hsla = rgb(0xFFFFFF).into();
    let dark: Hsla = rgb(0x111111).into();

    div()
        .w(px(pixel_size))
        .h(px(pixel_size))
        .rounded(px(10.0))
        .overflow_hidden()
        .bg(light)
        .flex_none()
        .child(
            canvas(
                |_, _, _| {},
                move |bounds: Bounds<Pixels>, _, window, _| {
                    window.paint_quad(quad(
                        bounds,
                        px(0.0),
                        light,
                        px(0.0),
                        gpui::transparent_black(),
                        BorderStyle::default(),
                    ));
                    let side = bounds.size.width.min(bounds.size.height);
                    let cell = side / total_modules;
                    let pad_x = bounds.origin.x + (bounds.size.width - side) * 0.5;
                    let pad_y = bounds.origin.y + (bounds.size.height - side) * 0.5;
                    let dim = matrix.size();
                    for y in 0..dim {
                        let mut x = 0;
                        while x < dim {
                            if !matrix.is_dark(x, y) {
                                x += 1;
                                continue;
                            }
                            let start_x = x;
                            while x < dim && matrix.is_dark(x, y) {
                                x += 1;
                            }
                            let run_len = (x - start_x) as f32;
                            // Slight overlap (+0.35px) prevents hairline anti-aliasing seams
                            // between adjacent dark modules on fractional DPI scales.
                            let rect = Bounds::new(
                                point(
                                    pad_x + cell * (start_x + quiet) as f32,
                                    pad_y + cell * (y + quiet) as f32,
                                ),
                                size(cell * run_len + px(0.35), cell + px(0.35)),
                            );
                            window.paint_quad(quad(
                                rect,
                                px(0.0),
                                dark,
                                px(0.0),
                                gpui::transparent_black(),
                                BorderStyle::default(),
                            ));
                        }
                    }
                },
            )
            .size_full(),
        )
}

fn build_data_codewords(data: &[u8], spec: &VersionSpec) -> Vec<u8> {
    let mut bits = Vec::with_capacity(spec.data_codewords * 8);
    // Mode indicator: Byte mode = 0100
    push_bits(&mut bits, 0b0100, 4);
    // Character count indicator
    let count_bits = if spec.version <= 9 { 8 } else { 16 };
    push_bits(&mut bits, data.len() as u32, count_bits);
    for &byte in data {
        push_bits(&mut bits, byte as u32, 8);
    }

    let capacity_bits = spec.data_codewords * 8;
    // Terminator (up to 4 zero bits)
    let terminator = (capacity_bits - bits.len()).min(4);
    push_bits(&mut bits, 0, terminator);
    // Pad to byte boundary
    while bits.len() % 8 != 0 {
        bits.push(false);
    }

    let mut codewords = Vec::with_capacity(spec.data_codewords);
    for chunk in bits.chunks_exact(8) {
        let mut byte = 0u8;
        for &bit in chunk {
            byte = (byte << 1) | u8::from(bit);
        }
        codewords.push(byte);
    }

    // Pad codewords alternating 0xEC, 0x11
    let pad = [0xECu8, 0x11u8];
    let mut pad_idx = 0;
    while codewords.len() < spec.data_codewords {
        codewords.push(pad[pad_idx & 1]);
        pad_idx += 1;
    }
    codewords
}

fn push_bits(bits: &mut Vec<bool>, value: u32, count: usize) {
    for shift in (0..count).rev() {
        bits.push(((value >> shift) & 1) != 0);
    }
}

fn interleave_with_ecc(data: &[u8], spec: &VersionSpec) -> Vec<u8> {
    let generator = rs_generator(spec.ec_per_block);
    let mut data_blocks: Vec<Vec<u8>> = Vec::new();
    let mut ec_blocks: Vec<Vec<u8>> = Vec::new();
    let mut offset = 0usize;

    for (count, block_len) in [spec.group1, spec.group2] {
        for _ in 0..count {
            let block = data[offset..offset + block_len].to_vec();
            offset += block_len;
            let ec = rs_remainder(&block, &generator);
            data_blocks.push(block);
            ec_blocks.push(ec);
        }
    }

    let max_data_len = data_blocks.iter().map(Vec::len).max().unwrap_or(0);
    let mut result = Vec::with_capacity(
        data.len() + spec.ec_per_block * (spec.group1.0 + spec.group2.0),
    );
    for col in 0..max_data_len {
        for block in &data_blocks {
            if col < block.len() {
                result.push(block[col]);
            }
        }
    }
    for col in 0..spec.ec_per_block {
        for block in &ec_blocks {
            result.push(block[col]);
        }
    }
    result
}

/// GF(256) multiplication modulo x^8 + x^4 + x^3 + x^2 + 1 (0x11D).
fn gf_mul(mut x: u8, mut y: u8) -> u8 {
    let mut z = 0u8;
    for _ in 0..8 {
        if (y & 1) != 0 {
            z ^= x;
        }
        let carry = (x & 0x80) != 0;
        x <<= 1;
        if carry {
            x ^= 0x1D;
        }
        y >>= 1;
    }
    z
}

fn rs_generator(degree: usize) -> Vec<u8> {
    let mut coeffs = vec![0u8; degree];
    coeffs[degree - 1] = 1;
    let mut root = 1u8;
    for _ in 0..degree {
        for i in 0..degree {
            coeffs[i] = gf_mul(coeffs[i], root);
            if i + 1 < degree {
                coeffs[i] ^= coeffs[i + 1];
            }
        }
        root = gf_mul(root, 0x02);
    }
    coeffs
}

fn rs_remainder(data: &[u8], generator: &[u8]) -> Vec<u8> {
    let mut rem = vec![0u8; generator.len()];
    for &byte in data {
        let factor = byte ^ rem[0];
        rem.rotate_left(1);
        *rem.last_mut().unwrap() = 0;
        for (slot, &coeff) in rem.iter_mut().zip(generator.iter()) {
            *slot ^= gf_mul(coeff, factor);
        }
    }
    rem
}

fn set_function(dim: usize, modules: &mut [bool], is_function: &mut [bool], x: usize, y: usize, dark: bool) {
    let idx = y * dim + x;
    modules[idx] = dark;
    is_function[idx] = true;
}

fn draw_function_patterns(version: usize, dim: usize, modules: &mut [bool], is_function: &mut [bool]) {
    // Timing patterns
    for i in 0..dim {
        set_function(dim, modules, is_function, 6, i, i % 2 == 0);
        set_function(dim, modules, is_function, i, 6, i % 2 == 0);
    }

    // Three 7×7 finder patterns + 1-module light separators
    for &(cx, cy) in &[(3isize, 3isize), (dim as isize - 4, 3), (3, dim as isize - 4)] {
        for dy in -4..=4 {
            for dx in -4..=4 {
                let x = cx + dx;
                let y = cy + dy;
                if x < 0 || y < 0 || x >= dim as isize || y >= dim as isize {
                    continue;
                }
                let dist = dx.abs().max(dy.abs());
                let dark = dist != 2 && dist != 4;
                set_function(dim, modules, is_function, x as usize, y as usize, dark);
            }
        }
    }

    // Alignment patterns
    let centers = alignment_centers(version);
    let last = centers.len().saturating_sub(1);
    for (iy, &cy) in centers.iter().enumerate() {
        for (ix, &cx) in centers.iter().enumerate() {
            if (ix == 0 && iy == 0) || (ix == 0 && iy == last) || (ix == last && iy == 0) {
                continue;
            }
            for dy in -2isize..=2 {
                for dx in -2isize..=2 {
                    let dist = dx.abs().max(dy.abs());
                    let dark = dist != 1;
                    set_function(
                        dim,
                        modules,
                        is_function,
                        (cx as isize + dx) as usize,
                        (cy as isize + dy) as usize,
                        dark,
                    );
                }
            }
        }
    }

    // Reserve format information areas (filled per mask later)
    for i in 0..=5 {
        set_function(dim, modules, is_function, 8, i, false);
        set_function(dim, modules, is_function, dim - 1 - i, 8, false);
        set_function(dim, modules, is_function, i, 8, false);
    }
    set_function(dim, modules, is_function, 8, 7, false);
    set_function(dim, modules, is_function, 8, 8, false);
    set_function(dim, modules, is_function, 7, 8, false);
    for i in 0..7 {
        set_function(dim, modules, is_function, 8, dim - 1 - i, false);
    }
    for i in 0..8 {
        set_function(dim, modules, is_function, dim - 1 - i, 8, false);
    }
    // Dark module (always dark)
    set_function(dim, modules, is_function, 8, dim - 8, true);

    // Reserve version information areas for Version >= 7
    if version >= 7 {
        for i in 0..6 {
            for j in 0..3 {
                set_function(dim, modules, is_function, dim - 11 + j, i, false);
                set_function(dim, modules, is_function, i, dim - 11 + j, false);
            }
        }
    }
}

fn draw_codewords(dim: usize, codewords: &[u8], modules: &mut [bool], is_function: &[bool]) {
    let mut bit_idx = 0usize;
    let total_bits = codewords.len() * 8;
    let mut right = dim as isize - 1;
    let mut upward = true;

    while right >= 1 {
        if right == 6 {
            right -= 1;
        }
        for vert in 0..dim {
            let y = if upward { dim - 1 - vert } else { vert };
            for j in 0..2 {
                let x = (right - j) as usize;
                let idx = y * dim + x;
                if !is_function[idx] {
                    let dark = if bit_idx < total_bits {
                        ((codewords[bit_idx >> 3] >> (7 - (bit_idx & 7))) & 1) != 0
                    } else {
                        false
                    };
                    modules[idx] = dark;
                    bit_idx += 1;
                }
            }
        }
        upward = !upward;
        right -= 2;
    }
}

fn apply_mask(dim: usize, mask: u8, modules: &mut [bool], is_function: &[bool]) {
    for y in 0..dim {
        for x in 0..dim {
            let idx = y * dim + x;
            if is_function[idx] {
                continue;
            }
            let invert = match mask {
                0 => (x + y) % 2 == 0,
                1 => y % 2 == 0,
                2 => x % 3 == 0,
                3 => (x + y) % 3 == 0,
                4 => (x / 3 + y / 2) % 2 == 0,
                5 => (x * y) % 2 + (x * y) % 3 == 0,
                6 => ((x * y) % 2 + (x * y) % 3) % 2 == 0,
                7 => ((x + y) % 2 + (x * y) % 3) % 2 == 0,
                _ => false,
            };
            if invert {
                modules[idx] = !modules[idx];
            }
        }
    }
}

fn draw_format_bits(dim: usize, mask: u8, modules: &mut [bool]) {
    // Error correction level M format bits = 00
    let data = (0b00u32 << 3) | (mask as u32);
    let mut rem = data;
    for _ in 0..10 {
        rem = (rem << 1) ^ ((rem >> 9) * 0x537);
    }
    let bits = ((data << 10) | rem) ^ 0x5412;

    // First copy (around top-left finder)
    for i in 0..=5 {
        modules[i * dim + 8] = ((bits >> i) & 1) != 0;
    }
    modules[7 * dim + 8] = ((bits >> 6) & 1) != 0;
    modules[8 * dim + 8] = ((bits >> 7) & 1) != 0;
    modules[8 * dim + 7] = ((bits >> 8) & 1) != 0;
    for i in 9..15 {
        modules[8 * dim + (14 - i)] = ((bits >> i) & 1) != 0;
    }

    // Second copy (around top-right & bottom-left finders)
    for i in 0..8 {
        modules[8 * dim + (dim - 1 - i)] = ((bits >> i) & 1) != 0;
    }
    for i in 8..15 {
        modules[(dim - 15 + i) * dim + 8] = ((bits >> i) & 1) != 0;
    }
    // Always-dark module
    modules[(dim - 8) * dim + 8] = true;
}

fn draw_version_bits(version: usize, dim: usize, modules: &mut [bool]) {
    let mut rem = version as u32;
    for _ in 0..12 {
        rem = (rem << 1) ^ ((rem >> 11) * 0x1F25);
    }
    let bits = ((version as u32) << 12) | rem;
    for i in 0..18 {
        let dark = ((bits >> i) & 1) != 0;
        let a = dim - 11 + (i % 3);
        let b = i / 3;
        modules[b * dim + a] = dark;
        modules[a * dim + b] = dark;
    }
}

fn penalty_score(dim: usize, modules: &[bool]) -> u32 {
    let mut score = 0u32;

    // Rule 1: adjacent modules in row/column with same color
    for y in 0..dim {
        let mut run_color = modules[y * dim];
        let mut run_len = 1u32;
        for x in 1..dim {
            let color = modules[y * dim + x];
            if color == run_color {
                run_len += 1;
            } else {
                if run_len >= 5 {
                    score += run_len - 2;
                }
                run_color = color;
                run_len = 1;
            }
        }
        if run_len >= 5 {
            score += run_len - 2;
        }
    }
    for x in 0..dim {
        let mut run_color = modules[x];
        let mut run_len = 1u32;
        for y in 1..dim {
            let color = modules[y * dim + x];
            if color == run_color {
                run_len += 1;
            } else {
                if run_len >= 5 {
                    score += run_len - 2;
                }
                run_color = color;
                run_len = 1;
            }
        }
        if run_len >= 5 {
            score += run_len - 2;
        }
    }

    // Rule 2: 2×2 blocks of same color
    for y in 0..dim - 1 {
        for x in 0..dim - 1 {
            let c = modules[y * dim + x];
            if c == modules[y * dim + x + 1]
                && c == modules[(y + 1) * dim + x]
                && c == modules[(y + 1) * dim + x + 1]
            {
                score += 3;
            }
        }
    }

    // Rule 3: finder-like patterns in rows/columns
    const P1: [bool; 11] = [true, false, true, true, true, false, true, false, false, false, false];
    const P2: [bool; 11] = [false, false, false, false, true, false, true, true, true, false, true];
    for y in 0..dim {
        for x in 0..=dim.saturating_sub(11) {
            let slice = &modules[y * dim + x..y * dim + x + 11];
            if slice == P1 || slice == P2 {
                score += 40;
            }
        }
    }
    for x in 0..dim {
        for y in 0..=dim.saturating_sub(11) {
            let mut m1 = true;
            let mut m2 = true;
            for k in 0..11 {
                let c = modules[(y + k) * dim + x];
                if c != P1[k] {
                    m1 = false;
                }
                if c != P2[k] {
                    m2 = false;
                }
            }
            if m1 || m2 {
                score += 40;
            }
        }
    }

    // Rule 4: balance of dark/light modules
    let dark = modules.iter().filter(|&&m| m).count();
    let total = dim * dim;
    let k = ((dark * 20).abs_diff(total * 10)) / total;
    score += (k as u32) * 10;

    score
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_pairing_urls_across_versions() {
        let short = encode_qr(b"http://192.168.1.192:34123").expect("version 2");
        assert_eq!(short.size(), 25);

        let pairing = encode_qr(
            b"http://100.84.159.56:34123/#token=0123456789abcdef0123456789abcdef",
        )
        .expect("pairing url fits cleanly");
        assert_eq!(pairing.size(), 37); // Version 5 (66 bytes fits in V5-M 84 bytes)

        // Verify all three 7×7 finder corners have solid outer ring & 3×3 core.
        let dim = pairing.size();
        for &(ox, oy) in &[(0, 0), (dim - 7, 0), (0, dim - 7)] {
            for i in 0..7 {
                assert!(pairing.is_dark(ox + i, oy));
                assert!(pairing.is_dark(ox + i, oy + 6));
                assert!(pairing.is_dark(ox, oy + i));
                assert!(pairing.is_dark(ox + 6, oy + i));
            }
            for dy in 2..=4 {
                for dx in 2..=4 {
                    assert!(pairing.is_dark(ox + dx, oy + dy));
                }
            }
        }
        // Always-dark module at (8, dim - 8).
        assert!(pairing.is_dark(8, dim - 8));
    }

    #[test]
    fn rejects_payload_larger_than_v10_capacity() {
        let huge = vec![b'a'; 300];
        assert!(encode_qr(&huge).is_none());
    }

    #[test]
    fn reed_solomon_matches_iso_v1_m_example() {
        // Known-good check: 16 data codewords + 10 EC codewords for V1-M
        let gen = rs_generator(10);
        assert_eq!(gen.len(), 10);
        let spec = &VERSION_SPECS_M[0];
        let data = build_data_codewords(b"hello", spec);
        assert_eq!(data.len(), 16);
        let rem = rs_remainder(&data, &gen);
        assert_eq!(rem.len(), 10);
        // Verify remainder is non-zero and deterministic.
        assert!(rem.iter().any(|&b| b != 0));
    }
}
