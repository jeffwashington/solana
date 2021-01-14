use std::time::{Duration, Instant};

const sz: usize = 10_000;

fn abc() {
    let start = Instant::now();
    let size = sz;
    let count = 5;
    let target_size = size * count;
    let y: Vec<u64> = (0..count).flat_map(|_| vec![1; size]).collect();
    let duration = start.elapsed();    
    println!("y i: {:?}", y.len());
    println!("elapsed: {:?}", duration);
}

fn def() {
    let start = Instant::now();
    let size = sz;
    let count = 5;
    let target_size = size * count;
    let mut y = Vec::<u64>::with_capacity(target_size);
    for _v in 0..count {
        y.extend(vec![1; size]);
    }
    let duration = start.elapsed();    
    println!("y i: {:?}", y.len());
    println!("elapsed: {:?}", duration);
}

fn def2() {
    let start = Instant::now();
    let size = sz;
    let count = 5;
    let target_size = size * count;
    let mut y = Vec::<u64>::with_capacity(target_size);
    (0..count).map(|_| vec![1; size]).map(|x| {y.extend(x)}).count();
    let duration = start.elapsed();    
    println!("y i: {:?}", y.len());
    println!("elapsed: {:?}", duration);
}

// This is the main function
fn main() {
    def2();
    abc();
    def();
}
