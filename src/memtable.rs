use format;

use log::FileLogReader;
use log::FileLogWriter;
use log::LogReader;
use log::LogWriter;
use std::collections::BTreeMap;
use std::io;
use std::path;

pub struct MemTable {
    logger: Box<LogWriter>,
    data: Box<BTreeMap<u64, u64>>
}

impl MemTable {
    pub fn record(&mut self, k: u64, v: u64) -> io::Result<()> {
        let mut buf : [u8; 16] = [0; 16];
        format::store(k, &mut buf[0..8]);
        format::store(v, &mut buf[8..16]);
        try!(self.logger.append(&buf));
        self.data.insert(k, v);
        return Ok(());
    }

    pub fn lookup(&self, k: u64) -> Option<&u64> {
        return self.data.get(&k);
    }

    pub fn create<P: AsRef<path::Path>>(filename: P) -> io::Result<MemTable> {
        return Ok(MemTable{
            logger: Box::new(try!(FileLogWriter::create(filename, 16))),
            data: Box::new(BTreeMap::new()),
        })
    }
    
    pub fn replay<P: AsRef<path::Path>>(filename: P) -> io::Result<BTreeMap<u64, u64>> {
        let mut data : BTreeMap<u64, u64> = BTreeMap::new();
        {
            let mut reader = try!(FileLogReader::create(&filename, 16));
            let mut buf : [u8; 16] = [0; 16];
            while try!(reader.next_record(&mut buf)) {
                let k : u64 = format::load(&buf[0..8]);
                let v : u64 = format::load(&buf[8..16]);
                data.insert(k, v);
            }
        }
        
        return Ok(data);
    }
}


#[cfg(test)]
mod test {
    use std::fs;
    use super::MemTable;
    
    #[test]
    fn single_recovery() {
        let filename = "/tmp/memtable";
        {
            let mut memtable = MemTable::create(filename).unwrap();
            memtable.record(1234, 5678).unwrap();
            assert_eq!(5678, *memtable.lookup(1234).unwrap());
        }

        {
            let map = MemTable::replay(filename)
                .expect("MemTable::replay");
            assert_eq!(Some(&5678), map.get(&1234));
        }

        fs::remove_file(&filename).unwrap();
    }

}
