use std::io;
use std::cmp::min;

pub trait Device {
    fn block_size(&self) -> u64;
    fn size(&self) -> u64;
    fn write(&mut self, offset: u64, &[u8]) -> io::Result<()>;
    fn read(&mut self, offset: u64, length: u64, buf: &mut [u8]) -> io::Result<usize>;
}

pub struct InMemoryDevice {
    size: u64,
    block_size: u64,
    data: Vec<u8>,
}

impl Device for InMemoryDevice {
    fn size(&self) -> u64 {
        return self.size
    }

    fn block_size(&self) -> u64 {
        return self.block_size
    }

    fn write(&mut self, offset: u64, data: &[u8]) -> io::Result<()> {
        if (data.len() as u64) % self.block_size != 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                                      format!("Invalid length {}", data.len())))
        }

        if offset % self.block_size != 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                                      format!("Invalid offset {}", offset)))
        }

        let len = data.len() as usize;
        let offset = offset as usize;
        for i in 0..len {
            self.data[offset + i] = data[i]
        }
        return Ok(())
    }

    fn read(&mut self, offset: u64, length: u64, buf: &mut [u8]) -> io::Result<usize> {
        assert!(length <= (buf.len() as u64));
        let len = min(length, self.size - offset);

        if len % self.block_size != 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                                      format!("Invalid length {}", len)))
        }

        if offset % self.block_size != 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput,
                                      format!("Invalid offset {}", offset)))
        }

        for i in 0..len {
            buf[i as usize] = self.data[(offset + i) as usize];
        }
        return Ok(len as usize);
    }
}

impl InMemoryDevice {
    pub fn new(size: u64, block_size: u64) -> InMemoryDevice {
        InMemoryDevice{
            size: size,
            block_size: block_size,
            data: vec![0; size as usize]
        }
    }
}

#[cfg(test)]
mod test {
    use super::Device;
    use super::InMemoryDevice;

    use std::io::ErrorKind;

    #[test]
    fn basic_read_write() {
        let mut dev = InMemoryDevice::new(10240, 4);
        assert_eq!(10240, dev.size());
        dev.write(0, &[0, 1, 2, 3]).unwrap();

        let mut buf : [u8; 4] = [0; 4];
        assert_eq!(4, dev.read(0, 4, &mut buf).unwrap());
        assert_eq!([0, 1, 2, 3], buf);
    }

    #[test]
    fn alignment() {
        let mut dev = InMemoryDevice::new(10240, 4);

        let wres = dev.write(0, &[5, 6]);
        assert_eq!(ErrorKind::InvalidInput, wres.unwrap_err().kind());

        let wres = dev.write(2, &[5, 6, 7, 8]);
        assert_eq!(ErrorKind::InvalidInput, wres.unwrap_err().kind());

        let mut buf : [u8; 4] = [0; 4];
        assert_eq!(ErrorKind::InvalidInput,
                   dev.read(0, 2, &mut buf).unwrap_err().kind());

        assert_eq!(ErrorKind::InvalidInput,
                   dev.read(2, 4, &mut buf).unwrap_err().kind());
    }
}
