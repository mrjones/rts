use std::io;
use std::path;

pub struct FileManager {
    root: path::PathBuf,
    log_path: Option<String>,
    log_version: usize,
}

impl FileManager {
    pub fn open<P: AsRef<path::Path>>(directory: P) -> io::Result<FileManager> {
        return Ok(FileManager{
            root: directory.as_ref().to_path_buf(),
            log_version: 0,
            log_path: None,
        });
    }

    pub fn new_log_file(&mut self) -> String {
        let v = self.log_version;
        self.log_version += 1;

        let mut buf = self.root.clone();
        buf.push(format!("log_{}", v));
        let path = buf.to_str().unwrap().to_string();
        self.log_path = Some(path.clone());
        return path;
    }
    
    pub fn log(&self) -> Option<String> {
        return self.log_path.clone();
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn basic() {
        let mut fm = super::FileManager::open("/tmp/filemanager")
            .expect("FileManager::open");
        assert_eq!(None, fm.log());
        assert_eq!("/tmp/filemanager/log_0", fm.new_log_file());
        assert_eq!("/tmp/filemanager/log_1", fm.new_log_file());
        assert_eq!("/tmp/filemanager/log_1", fm.log().unwrap());
    }
}

