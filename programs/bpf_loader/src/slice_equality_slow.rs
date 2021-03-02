extern crate test;

fn eq<'a, T: PartialEq>(a: &'a [T], b: &'a [T]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    for i in range(0, a.len()) {
        if a[i] != b[i] {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod bench {
    use test::Bencher;

    #[bench]
    fn builtin_eq(b: &mut Bencher) {
        let mut u = vec!();
        let mut v = vec!();
        for i in range(0u, 1_000_000) {
            u.push(i);
            v.push(i);
        }

        let x = u.as_slice();
        let y = v.as_slice();

        b.iter(|| {
            assert!(x == y);
        })
    }

    #[bench]
    fn custom_eq(b: &mut Bencher) {
        let mut u = vec!();
        let mut v = vec!();
        for i in range(0u, 1_000_000) {
            u.push(i);
            v.push(i);
        }

        let x = u.as_slice();
        let y = v.as_slice();

        b.iter(|| {
            assert!(super::eq(x, y));
        })
    }
}

fn main() {}
