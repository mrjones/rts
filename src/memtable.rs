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

impl MemTable {
    pub fn record(&mut self, k: u64, v: u64) -> io::Result<()> {
        let mut buf : [u8; 16] = [0; 16];
        store(k, &mut buf[0..8]);
        store(v, &mut buf[8..16]);
        try!(self.logger.append(&buf));
        self.data.insert(k, v);
        return Ok(());
    }

    pub fn lookup(&self, k: u64) -> Option<&u64> {
        return self.data.get(&k);
    }
    
    pub fn replay<P: AsRef<path::Path>>(filename: P) -> io::Result<MemTable> {
        let mut data : BTreeMap<u64, u64> = BTreeMap::new();
        {
            let r = FileLogReader::create(&filename, 16);
            if r.is_ok() {
                let mut reader = r.unwrap();
                let mut buf : [u8; 16] = [0; 16];
                while try!(reader.next_record(&mut buf)) {
                    let k : u64 = load(&buf[0..8]);
                    let v : u64 = load(&buf[8..16]);
                    data.insert(k, v);
                }
            }
        }
        
        return Ok(MemTable{
            logger: Box::new(try!(FileLogWriter::create(&filename, 16))),
            data: Box::new(data),
        });
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
            let mut memtable = MemTable::replay(filename).unwrap();
            memtable.record(1234, 5678).unwrap();
            assert_eq!(5678, *memtable.lookup(1234).unwrap());
        }

        {
            let memtable = MemTable::replay(filename).unwrap();
            assert_eq!(5678, *memtable.lookup(1234).unwrap());
        }

        fs::remove_file(&filename).unwrap();
    }

}
