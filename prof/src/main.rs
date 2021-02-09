use std::time::Instant;

fn main() {
let mut factor = 1;
let mut size = 0;
loop {
    factor = factor * 10;
    if factor > 5_000_000 {
        break;
    }
    size += 10;

    let sample = vec![0u8; size * 15_000_000];
    let now = Instant::now();
    let mut sum2 = 0;
    let factor = 1;
    for i in 0..sample.len() {
        if sample[i] != 1 {
            sum2 += 1;
        }
    }
    let then = Instant::now();
    let dur = then - now;
    println!("sum2: {}, time: {:?}, factor: {}, size: {}", sum2, dur, factor, size);
}}
