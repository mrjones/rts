use format;
use std::fs;
use std::io;
use std::io::Read;
use std::io::Write;
use std::path;

pub struct TableBuilder;

const BLOCK_SIZE : usize = 32768;
const FOOTER_SIZE : usize = 8;
const REC_SIZE : usize = 16;

impl TableBuilder {
    pub fn write<'a, P: AsRef<path::Path>, I: Iterator<Item=(&'a u64, &'a u64)>>(filename: P, data: I) -> io::Result<()> {
        let mut file = try!(fs::File::create(filename));

        let mut rec_count = 0;
        let mut block = [0; BLOCK_SIZE];
        let mut block_ptr = 0;

        let mut prev_k = 0;
        
        for (k, v) in data {
            if rec_count > 0 && *k < prev_k {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Keys must be ordered. {} is not greater than {}",
                            *k, prev_k)));
            }
            prev_k = *k;
            
            assert!(block_ptr + REC_SIZE <= (BLOCK_SIZE - FOOTER_SIZE));
            format::store(*k, &mut block[block_ptr..(block_ptr + 8)]);
            format::store(*v, &mut block[(block_ptr + 8)..(block_ptr+16)]);
            rec_count += 1;
            block_ptr += REC_SIZE;
            if BLOCK_SIZE - FOOTER_SIZE - block_ptr < REC_SIZE {
                try!(TableBuilder::pad_and_write(&mut block, block_ptr, rec_count, &mut file));
                block_ptr = 0;
                rec_count = 0;
            }
        }

        try!(TableBuilder::pad_and_write(&mut block, block_ptr, rec_count, &mut file));
        
        return Ok(());
    }

    fn pad_and_write(block : &mut [u8; BLOCK_SIZE], block_ptr: usize, rec_count : usize, file: &mut fs::File) -> io::Result<()> {
        let bytes_remaining = BLOCK_SIZE - FOOTER_SIZE - block_ptr;
        for i in 0..bytes_remaining {
            block[block_ptr + i] = 0;
        }

        let footer_ptr = BLOCK_SIZE - FOOTER_SIZE;
        format::store(rec_count as u64, &mut block[footer_ptr..(footer_ptr+8)]);
        
        try!(file.write_all(block));
        return Ok(());
    }
}

pub struct TableIterator {
    block: [u8; BLOCK_SIZE],
    status: io::Result<()>,
    block_ptr: usize,
    file: fs::File,
    records_in_block: usize,
    records_read_from_block: usize,
    done: bool,
}

impl TableIterator {
    pub fn new<P: AsRef<path::Path>>(filename: P) -> io::Result<TableIterator> {
        let f = try!(fs::File::open(filename));

        return Ok(TableIterator{
            block: [0; BLOCK_SIZE],
            status: Ok(()),
            block_ptr: BLOCK_SIZE,
            file: f,
            records_in_block: 0,
            records_read_from_block: 0,
            done: false,
        });
    }

    fn read_block(&mut self) -> io::Result<()> {
        println!("Read block");
        let read_size = try!(self.file.read(&mut self.block));
        if read_size == 0 {
            self.done = true;
            return Ok(());
        }
        assert_eq!(read_size, BLOCK_SIZE);

        let footer_ptr = BLOCK_SIZE - FOOTER_SIZE;
        self.records_in_block = format::load(&self.block[footer_ptr..(footer_ptr+8)]) as usize;

        self.block_ptr = 0;
        return Ok(());
    }
}

impl Iterator for TableIterator {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<(u64, u64)> {
        if self.done {
            return None;
        }

        if self.records_read_from_block >= self.records_in_block {           
            self.status = self.read_block();
            if !self.status.is_ok() {
                self.done = true;
                return None;
            }

            if self.done {
                return None;
            }

        }

        let k = format::load(&self.block[self.block_ptr..(self.block_ptr+8)]);
        let v = format::load(&self.block[(self.block_ptr+8)..(self.block_ptr+16)]);
        self.block_ptr += REC_SIZE;
        self.records_read_from_block += 1;

        return Some((k,v));
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::io;

    #[test]
    fn write_table() {
        let mut map = BTreeMap::new();
        for i in 0..1000 {
            map.insert(i, i+1);
        }
        
        {
            super::TableBuilder::write("/tmp/table", map.iter())
                .expect("TableWriter::write");
        }

        let mut iter = super::TableIterator::new("/tmp/table")
            .expect("TableIterator::new");
        for i in 0..1000 {
            assert_eq!((i,i+1), iter.next().expect(&format!("Val {}", i)));
        }

        assert_eq!(None, iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn unordered_records() {
        let mut map = HashMap::new();
        for i in 0..1000 {
            map.insert(i, i);
        }

        let res = super::TableBuilder::write("/tmp/table-unordered", map.iter());

        assert!(res.is_err());
        assert_eq!(io::ErrorKind::InvalidInput, res.unwrap_err().kind());
    }
}
