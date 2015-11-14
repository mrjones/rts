trait Device {
    fn size(&self) -> i64;
}

struct InMemoryDevice {
    size: i64
}

impl Device for InMemoryDevice {
    fn size(&self) -> i64 {
        return self.size
    }
}

#[test]
fn in_memory() {
    let imd = InMemoryDevice{size: 10240};
    assert_eq!(10240, imd.size());
}
