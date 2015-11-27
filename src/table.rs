use format;
use std::fs;
use std::io;
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
        
        for (k, v) in data {
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

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    #[test]
    fn write_table() {
        let mut map = BTreeMap::new();
        map.insert(1, 2);
        map.insert(3, 4);

        super::TableBuilder::write("/tmp/table", map.iter()).unwrap();
    }
}
