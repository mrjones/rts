use std::io;
use std::cmp::min;

pub trait Device {
    fn size(&self) -> u64;
    fn write(&mut self, offset: u64, &[u8]) -> io::Result<()>;
    fn read(&mut self, offset: u64, length: u64, buf: &mut [u8]) -> io::Result<usize>;
}

pub struct InMemoryDevice {
    size: u64,
    data: Vec<u8>,
}

impl Device for InMemoryDevice {
    fn size(&self) -> u64 {
        return self.size
    }

    fn write(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        let len = data.len() as usize;
        let offset = offset as usize;
        for i in 0..len {
            self.data[offset + i] = data[i]
        }
        Ok(())
    }

    fn read(&mut self, offset: u64, length: u64, buf: &mut [u8]) -> io::Result<usize> {
        assert!(length >= buf.len() as u64);
        let len = min(length, self.size - offset);
        for i in 0..len {
            buf[i as usize] = self.data[(offset + i) as usize];
        }
        let x: io::Result<usize> = Ok(len as usize);
        x
    }
}

impl InMemoryDevice {
    pub fn new(size: u64) -> InMemoryDevice {
        InMemoryDevice{
            size: size,
            data: vec![0; size as usize]
        }
    }
}

#[cfg(test)]
mod test {
    use super::Device;
    use super::InMemoryDevice;

    #[test]
    fn in_memory() {
        let mut dev = InMemoryDevice::new(10240);
        assert_eq!(10240, dev.size());
        let _ = dev.write(0, &[0, 1, 2, 3]).unwrap();

        let mut buf : [u8; 4] = [0; 4];
        assert_eq!(4, dev.read(0, 4, &mut buf).unwrap());
        assert_eq!([0, 1, 2, 3], buf);
    }
}
