pub fn chunk_and_pad_pow2(input: &[u8], chunk_bytes: usize) -> (Vec<Vec<u8>>, u64, u32) {
    let original_len = input.len() as u64;

    let mut chunks: Vec<Vec<u8>> = Vec::new();
    let mut i = 0usize;
    while i < input.len() {
        let end = (i + chunk_bytes).min(input.len());
        chunks.push(input[i..end].to_vec());
        i = end;
    }

    let real = chunks.len().max(1);
    let padded = next_pow2(real);
    while chunks.len() < padded {
        chunks.push(Vec::new()); // pad chunk = empty payload
    }

    (chunks, original_len, padded as u32)
}

pub fn next_pow2(mut n: usize) -> usize {
    if n <= 1 {
        return 1;
    }
    n -= 1;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    if usize::BITS == 64 {
        n |= n >> 32;
    }
    n + 1
}
