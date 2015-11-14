trait Device {
    fn size(&self) -> i64;
}

struct InMemoryDevice {
    size: i64,
}

impl Device for InMemoryDevice {
    fn size(&self) -> i64 {
        return self.size
    }
}

impl InMemoryDevice {
    fn new(size: i64) -> InMemoryDevice {
        InMemoryDevice{
            size: size
        }
    }
}

#[cfg(test)]
mod test {
    use super::Device;
    use super::InMemoryDevice;

    #[test]
    fn in_memory() {
        let dev = InMemoryDevice::new(10240);
        assert_eq!(10240, dev.size());
    }
}
