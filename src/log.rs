use std::fs;
use std::io;
use std::io::Read;
use std::io::Write;
use std::path;

const BLOCK_SIZE_BYTES : usize = 32768;

pub trait LogWriter {
    fn append(&mut self, buf: &[u8]) -> io::Result<()>;
}

pub trait LogReader {
    fn next_record(&mut self, result: &mut [u8]) -> io::Result<bool>;
}

pub struct FileLogWriter {
    file: fs::File,
    record_size_bytes: usize,
    block_ptr: usize,
}

impl FileLogWriter {
    pub fn create<P: AsRef<path::Path>>(path: P, record_size_bytes: usize) -> io::Result<FileLogWriter> {
        let f = try!(fs::File::create(path));
        return Ok(FileLogWriter{
            file: f,
            record_size_bytes: record_size_bytes,
            block_ptr: 0,
        })
    }
}

impl LogWriter for FileLogWriter {
    fn append(&mut self, buf: &[u8]) -> io::Result<()> {
        let bytes_remaining = BLOCK_SIZE_BYTES - self.block_ptr;
        if bytes_remaining < self.record_size_bytes {
            let zeroes = vec![0; bytes_remaining];
            try!(self.file.write_all(&zeroes[..]));
            self.block_ptr = 0;
        }
        
        if buf.len() != self.record_size_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Bad record size. Expected: {}. Got: {}",
                        self.record_size_bytes, buf.len())));
        }
        
        return self.file.write_all(buf);
    }
}

pub struct FileLogReader {
    file: fs::File,
    record_size_bytes: usize,
    buf: [u8; BLOCK_SIZE_BYTES],
    buf_ptr: usize,
    buf_size: usize,
    read_last_block: bool,
}

impl FileLogReader {
    pub fn create<P: AsRef<path::Path>>(path: P, record_size_bytes: usize) -> io::Result<FileLogReader> {
        let f = try!(fs::File::open(path));
        return Ok(FileLogReader{
            file: f,
            record_size_bytes: record_size_bytes,
            buf: [0; BLOCK_SIZE_BYTES],
            buf_ptr: 0,
            buf_size: 0,
            read_last_block: false,
        })
    }
}

impl LogReader for FileLogReader {
    fn next_record(&mut self, result: &mut [u8]) -> io::Result<bool> {
        if result.len() != self.record_size_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Bad record size. Expected: {}. Got: {}",
                        self.record_size_bytes, result.len())));
        }
        
        if self.current_block_expired() {
            if self.read_last_block {
                return Ok(false);
            }

            try!(self.read_next_block());
        }

        for i in 0..self.record_size_bytes {
            result[i] = self.buf[self.buf_ptr + i];
        }

        self.buf_ptr += self.record_size_bytes;
        return Ok(true);
    }
}

impl FileLogReader {
    fn read_next_block(&mut self) -> io::Result<()> {
        self.buf_ptr = 0;
        self.buf_size = try!(self.file.read(&mut self.buf));

        if self.buf_size < BLOCK_SIZE_BYTES {
            self.read_last_block = true;
        }
        return Ok(());
    }

    fn current_block_expired(&self) -> bool {
        return self.buf_ptr >= (self.buf_size - self.block_padding())
    }

    fn block_padding(&self) -> usize {
        return BLOCK_SIZE_BYTES % self.record_size_bytes;
    }
}

#[cfg(test)]
mod test {
    use super::FileLogReader;
    use super::FileLogWriter;
    use super::LogReader;
    use super::LogWriter;

    use std::fs;
    use std::io::ErrorKind;

    #[test]
    fn invalid_record_size() {
        let mut log = FileLogWriter::create("/tmp/filelog", 4).unwrap();
        assert_eq!(ErrorKind::InvalidInput,
                   log.append(&[0]).unwrap_err().kind());
        
    }
    
    #[test]
    fn single_log_replay() {
        {
            let mut writer = FileLogWriter::create("/tmp/filelog", 4).unwrap();
            writer.append(&[0,1,2,3]).unwrap();
        }

        let mut reader = FileLogReader::create("/tmp/filelog", 4).unwrap();
        let mut buf = [0; 4];
        assert_eq!(true, reader.next_record(&mut buf).unwrap());
        assert_eq!([0,1,2,3], buf);
        // TODO(mrjones): i'm not sure these are the semantics I want
        assert_eq!(false, reader.next_record(&mut buf).unwrap());

        fs::remove_file("/tmp/filelog").unwrap();
    }

    #[test]
    fn multiple_block_replay() {
        let records = 2 * ((super::BLOCK_SIZE_BYTES + 4) / 4);
        
        {
            let mut writer = FileLogWriter::create("/tmp/filelog.multi", 4).unwrap();
            for i in 0..records {
                let v = (i % 256) as u8;
                writer.append(&[v, v, v, v]).unwrap();
            }
        }

        let mut reader = FileLogReader::create("/tmp/filelog.multi", 4).unwrap();
        let mut buf = [0; 4];
        for i in 0..records {
            let v = (i % 256) as u8;
            assert_eq!(true, reader.next_record(&mut buf).unwrap());
            assert_eq!([v, v, v, v], buf);
        }
        assert_eq!(false, reader.next_record(&mut buf).unwrap());
        fs::remove_file("/tmp/filelog.multi").unwrap();
    }
}
