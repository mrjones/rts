use filemanager;
use format;
use std::path;
use std::io;

use block_storage::Device;

pub struct Db {
    storage: Box<Device>,
    filemanager: filemanager::FileManager,
}

fn record_offset(rec_index: u64) -> u64 {
    return format::VAL_WIDTH as u64 + rec_index * format::REC_WIDTH as u64;
}

// Stupid data layout
// First 8 bytes: count of records
// Each record is 16 bytes: 8 bytes of timestamp, 8 bytes of value

impl Db {
    pub fn new<P: AsRef<path::Path>>(storage: Box<Device>, directory: P) -> io::Result<Db> {
        return Ok(Db{
            storage: storage,
            filemanager: try!(filemanager::FileManager::open_or_create(directory)),
        });
    }

    pub fn record(&mut self, rec: &format::Rec) -> io::Result<()> {
        let rec_count = try!(self.num_records());

        let offset = record_offset(rec_count);
        let mut buf : [u8; format::REC_WIDTH] = [0; format::REC_WIDTH];
        format::store_rec(&rec, &mut buf);
        try!(self.storage.write(offset, &buf));
        try!(self.set_record_count(rec_count + 1));
        
        return Ok(());
    }

    pub fn lookup(&mut self, ts: u64) -> io::Result<u64> {
        let rec_count = try!(self.num_records());

        let mut buf : [u8; format::REC_WIDTH] = [0; format::REC_WIDTH];
        for i in 0..rec_count {
            let offset = record_offset(i);
            try!(self.storage.read(offset, format::REC_WIDTH as u64, &mut buf));
            let rec = format::load_rec(&buf);
            if rec.timestamp == ts {
                return Ok(rec.value);
            }
        }

        return Err(io::Error::new(io::ErrorKind::NotFound, "No Matching TS"));
    }

    fn num_records(&mut self) -> io::Result<u64> {
        let mut rec_count_buf : [u8; format::VAL_WIDTH] = [0; format::VAL_WIDTH];
        try!(self.storage.read(0, format::VAL_WIDTH as u64, &mut rec_count_buf));
        return Ok(format::load(&rec_count_buf));
    }

    fn set_record_count(&mut self, rec_count: u64) -> io::Result<()> {
        let mut rec_count_buf : [u8; format::VAL_WIDTH] = [0; format::VAL_WIDTH];
        format::store(rec_count + 1, &mut rec_count_buf);
        try!(self.storage.write(0, &rec_count_buf));
        return Ok(())
    }
}

#[cfg(test)]
mod test {
    extern crate time;

    use block_storage::InMemoryDevice;
    use format;
    use std::io::ErrorKind;
    
    use super::Db;

    #[test]
    fn db_test() {
        let mut db = Db::new(Box::new(InMemoryDevice::new(10240, 4)), "/tmp/db")
            .expect("Db::new");
        db.record(&format::Rec{timestamp: 1234567890, value: 257}).unwrap();
        db.record(&format::Rec{timestamp: 1111111111, value: 1}).unwrap();

        assert_eq!(257, db.lookup(1234567890).unwrap());
        assert_eq!(1,   db.lookup(1111111111).unwrap());
        assert_eq!(ErrorKind::NotFound,
                   db.lookup(2222222222).unwrap_err().kind());
    }
}
