fn foo() -> u64 {
    let mut factor = 1_000_000;
    let mut size = 60;
    
    let sample = vec![0u8; size * 15_000_000];
    let mut sum2 = 0;
    let factor = 1;
    for i in 0..(sample.len()/size) {
        if sample[i*size] != 1 {
            sum2 += 1;
        }
    }
    sum2
}
fn main() {
    println!("{}", foo());
}
