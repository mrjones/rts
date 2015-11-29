use filemanager;
use format;
use memtable;
use table;

use std::path;
use std::io;

pub struct Db {
    filemanager: Box<filemanager::FileManager>,
    memtable: Box<memtable::MemTable>,
}

// TODO(mrjones): concurrency
impl Db {
    pub fn new<P: AsRef<path::Path>>(directory: P) -> io::Result<Db> {
        let mut fm = Box::new(try!(
            filemanager::FileManager::open_or_create(directory)));

        match fm.latest_log() {
            Some(filename) => {
                let data = try!(memtable::MemTable::replay(filename));
                println!("Compacting recovered log: {:?}", data);

                
                try!(table::TableBuilder::write(
                    fm.new_table_file(), data.iter()));
                // Delete obsolete log?
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
        // TODO(mrjones): periodically compact the log
        // TODO(mrjones): periodically merge tables
        return self.memtable.record(rec.timestamp, rec.value);
    }

    pub fn lookup(&mut self, ts: u64) -> io::Result<u64> {
        match self.memtable.lookup(ts) {
            Some(v) => return Ok(*v),
            None => (),
        }

        // TODO(mrjones): binary search the tables
        for filename in self.filemanager.table_paths() {
            for (k, v) in try!(table::TableIterator::new(filename)) {
                if k == ts {
                    return Ok(v);
                }
            }
        }

        return Err(io::Error::new(io::ErrorKind::NotFound, "No Matching TS"));
    }
}

#[cfg(test)]
mod test {
    extern crate time;

    use format;
    
    use std::fs;
    use std::io;
    
    use super::Db;

    fn accept_not_found(err: io::Error) -> io::Result<()> {
        if err.kind() == io::ErrorKind::NotFound {
            return Ok(());
        }
        return Err(err);
    }

    #[test]
    fn db_test() {
        fs::remove_dir_all("/tmp/db").or_else(accept_not_found).unwrap();

        let mut db = Db::new("/tmp/db")
            .expect("Db::new");
        db.record(&format::Rec{timestamp: 1234567890, value: 257}).unwrap();
        db.record(&format::Rec{timestamp: 1111111111, value: 1}).unwrap();

        assert_eq!(257, db.lookup(1234567890).unwrap());
        assert_eq!(1,   db.lookup(1111111111).unwrap());
        assert_eq!(io::ErrorKind::NotFound,
                   db.lookup(2222222222).unwrap_err().kind());
    }

    #[test]
    fn recovery() {
        fs::remove_dir_all("/tmp/db2").or_else(accept_not_found).unwrap();

        {
            let mut db = Db::new("/tmp/db2").expect("Db::new");
            db.record(&format::Rec{timestamp: 1234567890, value: 257}).unwrap();
            db.record(&format::Rec{timestamp: 1111111111, value: 1}).unwrap();

            assert_eq!(257, db.lookup(1234567890).unwrap());
            assert_eq!(1,   db.lookup(1111111111).unwrap());
            assert_eq!(io::ErrorKind::NotFound,
                       db.lookup(2222222222).unwrap_err().kind());
        }

        {
            let mut db = Db::new("/tmp/db2").expect("Db::new");

            assert_eq!(257, db.lookup(1234567890).unwrap());
            assert_eq!(1,   db.lookup(1111111111).unwrap());
            assert_eq!(io::ErrorKind::NotFound,
                       db.lookup(2222222222).unwrap_err().kind());
        }
    }
}
