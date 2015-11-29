use filemanager;
use format;
use memtable;
use std::path;
use std::io;

pub struct Db {
    filemanager: Box<filemanager::FileManager>,
    memtable: Box<memtable::MemTable>,
}

impl Db {
    pub fn new<P: AsRef<path::Path>>(directory: P) -> io::Result<Db> {
        let mut fm = Box::new(try!(filemanager::FileManager::open_or_create(directory)));

        match fm.latest_log() {
            Some(filename) => {
                let data = try!(memtable::MemTable::replay(filename));
                println!("Recovered: {:?}", data);

                // TODO(mrjones): do something with recovered data
            },
            None => (),
        }

        let log_file_name = fm.new_log_file();
        
        return Ok(Db{
            filemanager: fm,
            memtable: Box::new(try!(memtable::MemTable::create(log_file_name))),
        });
    }

    pub fn record(&mut self, rec: &format::Rec) -> io::Result<()> {
        return self.memtable.record(rec.timestamp, rec.value);
    }

    pub fn lookup(&mut self, ts: u64) -> io::Result<u64> {
        match self.memtable.lookup(ts) {
            Some(v) => return Ok(*v),
            None => return Err(io::Error::new(io::ErrorKind::NotFound, "No Matching TS")),
        }

        // TODO(mrjones): check sorted tables
    }
}

#[cfg(test)]
mod test {
    extern crate time;

    use format;
    use std::io::ErrorKind;
    
    use super::Db;

    #[test]
    fn db_test() {
        let mut db = Db::new("/tmp/db")
            .expect("Db::new");
        db.record(&format::Rec{timestamp: 1234567890, value: 257}).unwrap();
        db.record(&format::Rec{timestamp: 1111111111, value: 1}).unwrap();

        assert_eq!(257, db.lookup(1234567890).unwrap());
        assert_eq!(1,   db.lookup(1111111111).unwrap());
        assert_eq!(ErrorKind::NotFound,
                   db.lookup(2222222222).unwrap_err().kind());
    }
}
