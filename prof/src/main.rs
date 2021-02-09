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
    let mut sum = 0;
    let mut sum2 = 0;
    let factor = 1;
    (0..sample.len()/(size * factor)).into_iter().for_each(|i|{
        for j in 0..factor {
            if sample[i*size*factor+j*size] != 1 {
                sum2 += 1;
            }

        }
    }
    );
    let then = Instant::now();
    let dur = then - now;
    println!("sum2: {}, time: {:?}, l: {}, factor: {}, size: {}", sum, dur, sum2, factor, size);
}}
