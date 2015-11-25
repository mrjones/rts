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
}

impl FileLogWriter {
    pub fn create<P: AsRef<path::Path>>(path: P, record_size_bytes: usize) -> io::Result<FileLogWriter> {
        let f = try!(fs::File::create(path));
        return Ok(FileLogWriter{
            file: f,
            record_size_bytes: record_size_bytes,
        })
    }
}

impl LogWriter for FileLogWriter {
    fn append(&mut self, buf: &[u8]) -> io::Result<()> {
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

    done: bool,
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
            done: false,
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
        
        if self.done {
            return Ok(true);
        }

        if self.buf_ptr >= (self.buf_size - self.block_padding()) {
            try!(self.read_next_block());
        }

        for i in 0..self.record_size_bytes {
            result[i] = self.buf[self.buf_ptr + i];
        }

        self.buf_ptr += self.record_size_bytes;

        // TODO(mrjons): Check done
        return Ok(false);
    }
}

impl FileLogReader {
    fn read_next_block(&mut self) -> io::Result<()> {
        self.buf_ptr = 0;
        self.buf_size = try!(self.file.read(&mut self.buf));
        return Ok(());
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
    
    use std::io::ErrorKind;

    #[test]
    fn invalid_record_size() {
        let mut log = FileLogWriter::create("/tmp/filelog", 4).unwrap();
        assert_eq!(ErrorKind::InvalidInput,
                   log.append(&[0]).unwrap_err().kind());
        
    }
    
    #[test]
    fn basic_log_replay() {
        {
            let mut writer = FileLogWriter::create("/tmp/filelog", 4).unwrap();
            writer.append(&[0,1,2,3]).unwrap();
        }

        let mut reader = FileLogReader::create("/tmp/filelog", 4).unwrap();
        let mut buf = [0; 4];
        reader.next_record(&mut buf).unwrap();
    }
}
