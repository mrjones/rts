use std::io;

use block_storage::Device;

pub struct Db {
    storage: Box<Device>
}

const VAL_WIDTH: usize = 8;

fn load(buf: &[u8]) -> u64 {
    assert_eq!(VAL_WIDTH, buf.len());

    let mut acc : u64 = 0;
    for b in 0..VAL_WIDTH {
        acc += (buf[b] as u64) << (8 * b);
    }
    return acc;
}

fn store(n: u64, buf: &mut [u8]) {
    assert_eq!(VAL_WIDTH, buf.len());

    for b in 0..VAL_WIDTH {
        buf[b] = ((n >> (8 * b)) % 256) as u8;
    }
}

impl Db {
    pub fn new(storage: Box<Device>) -> Db {
        return Db{storage: storage}
    }

    pub fn record(&mut self, ts: u64, val: u64) -> io::Result<()> {
//        try!(self.storage.read(0, VAL_WIDTH));

        let mut rec : [u8; 2 * VAL_WIDTH] = [0; 2 * VAL_WIDTH];
        store(ts, &mut rec[0..VAL_WIDTH]);
        store(val, &mut rec[VAL_WIDTH..(2*VAL_WIDTH)]);
        try!(self.storage.write(0, &rec));
        return Ok(());
    }

    pub fn lookup(&mut self, ts: u64) -> io::Result<u64> {
        let mut rec : [u8; 2 * VAL_WIDTH] = [0; 2 * VAL_WIDTH];
        try!(self.storage.read(0, (2 * VAL_WIDTH) as u64, &mut rec));
        let disk_ts = load(&rec[0..VAL_WIDTH]);
        if disk_ts != ts {
            return Err(io::Error::new(io::ErrorKind::Other, "mismatched ts"));
        }

        let disk_val = load(&rec[VAL_WIDTH..(2 * VAL_WIDTH)]);

        return Ok(disk_val);
    }
}

#[cfg(test)]
mod test {
    use block_storage::InMemoryDevice;
    use super::Db;
    use super::load;
    use super::store;
    use super::VAL_WIDTH;

    fn round_trip(n: u64) -> u64{
        let mut buf : [u8; VAL_WIDTH] = [0; VAL_WIDTH];

        store(n, &mut buf);
        return load(&buf);
    }

    #[test]
    fn encode_decode() {
        assert_eq!(0, round_trip(0));
        assert_eq!(1, round_trip(1));
        assert_eq!(255, round_trip(255));
        assert_eq!(256, round_trip(256));
        assert_eq!(1234567890, round_trip(1234567890));
        assert_eq!(u64::max_value(), round_trip(u64::max_value()));
    }

    #[test]
    fn db_test() {
        let mut db = Db::new(Box::new(InMemoryDevice::new(10240, 4)));
        db.record(1234567890, 257).unwrap();
        assert_eq!(257, db.lookup(1234567890).unwrap());
    }
}
