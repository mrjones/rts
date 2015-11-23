use std::io;

use block_storage::Device;

pub struct Db {
    storage: Box<Device>
}

const VAL_WIDTH: usize = 8;
const REC_WIDTH: usize = 2 * VAL_WIDTH;

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

#[derive(Debug,PartialEq)]
pub struct Rec {
    timestamp: u64,
    value: u64,
}

fn store_rec(rec: &Rec, buf: &mut [u8]) {
    assert_eq!(2 * VAL_WIDTH, buf.len());

    store(rec.timestamp, &mut buf[0..VAL_WIDTH]);
    store(rec.value, &mut buf[VAL_WIDTH..(2*VAL_WIDTH)]);
}

fn load_rec(buf: &[u8]) -> Rec {
    assert_eq!(2 * VAL_WIDTH, buf.len());
    
    let value = load(&buf[VAL_WIDTH..(2*VAL_WIDTH)]);
    let timestamp = load(&buf[0..VAL_WIDTH]);

    return Rec{timestamp: timestamp, value: value};
}

fn record_offset(rec_index: u64) -> u64 {
    return VAL_WIDTH as u64 + rec_index * REC_WIDTH as u64;
}

// Stupid data layout
// First 8 bytes: count of records
// Each record is 16 bytes: 8 bytes of timestamp, 8 bytes of value

impl Db {
    pub fn new(storage: Box<Device>) -> Db {
        return Db{storage: storage}
    }

    pub fn record(&mut self, rec: &Rec) -> io::Result<()> {
        let rec_count = try!(self.num_records());

        let offset = record_offset(rec_count);
        let mut buf : [u8; REC_WIDTH] = [0; REC_WIDTH];
        store_rec(&rec, &mut buf);
        try!(self.storage.write(offset, &buf));
        try!(self.set_record_count(rec_count + 1));
        
        return Ok(());
    }

    pub fn lookup(&mut self, ts: u64) -> io::Result<u64> {
        let rec_count = try!(self.num_records());

        let mut buf : [u8; REC_WIDTH] = [0; REC_WIDTH];
        for i in 0..rec_count {
            let offset = record_offset(i);
            try!(self.storage.read(offset, (REC_WIDTH) as u64, &mut buf));
            let rec = load_rec(&buf);
            if rec.timestamp == ts {
                return Ok(rec.value);
            }
        }

        return Err(io::Error::new(io::ErrorKind::NotFound, "No Matching TS"));
    }

    fn num_records(&mut self) -> io::Result<u64> {
        let mut rec_count_buf : [u8; VAL_WIDTH] = [0; VAL_WIDTH];
        try!(self.storage.read(0, VAL_WIDTH as u64, &mut rec_count_buf));
        return Ok(load(&rec_count_buf));
    }

    fn set_record_count(&mut self, rec_count: u64) -> io::Result<()> {
        let mut rec_count_buf : [u8; VAL_WIDTH] = [0; VAL_WIDTH];
        store(rec_count + 1, &mut rec_count_buf);
        try!(self.storage.write(0, &rec_count_buf));
        return Ok(())
    }
}

#[cfg(test)]
mod test {
    extern crate time;
    
    use block_storage::InMemoryDevice;
    use std::io::ErrorKind;
    
    use super::Db;
    use super::Rec;
    
    use super::load;
    use super::load_rec;
    use super::store;
    use super::store_rec;

    use super::REC_WIDTH;
    use super::VAL_WIDTH;

    fn round_trip(n: u64) -> u64{
        let mut buf : [u8; VAL_WIDTH] = [0; VAL_WIDTH];

        store(n, &mut buf);
        return load(&buf);
    }

    fn round_trip_rec(rec: Rec) {
        let mut buf : [u8; REC_WIDTH] = [0; REC_WIDTH];
        store_rec(&rec, &mut buf);
        let rec2 = load_rec(&buf);
        assert_eq!(rec, rec2);
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
    fn encode_decode_rec() {
        round_trip_rec(Rec{
            timestamp: 1234567890,
            value: 257,
        });
    }

    #[test]
    fn db_test() {
        let mut db = Db::new(Box::new(InMemoryDevice::new(10240, 4)));
        db.record(&Rec{timestamp: 1234567890, value: 257}).unwrap();
        db.record(&Rec{timestamp: 1111111111, value: 1}).unwrap();

        assert_eq!(257, db.lookup(1234567890).unwrap());
        assert_eq!(1,   db.lookup(1111111111).unwrap());
        assert_eq!(ErrorKind::NotFound,
                   db.lookup(2222222222).unwrap_err().kind());
    }
}
