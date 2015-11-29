use std::fs;
use std::io;
use std::path;

pub struct FileManager {
    root: path::PathBuf,
    log_path: Option<String>,
    log_version: usize,
}

impl FileManager {
    pub fn open_or_create<P: AsRef<path::Path>>(dir: P) -> io::Result<FileManager> {
        let md = fs::metadata(dir.as_ref());

        match md {
            Err(err) => {
                if err.kind() != io::ErrorKind::NotFound {
                    return Err(err);
                } else {
                    try!(fs::create_dir(dir.as_ref()));
                }
            },
            Ok(md) => {
                if !md.is_dir() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("'{:?}' is not a directory.", dir.as_ref())));
                }
            }
        }
        
        for entry in try!(fs::read_dir(dir.as_ref())) {
            let entry = try!(entry);
//            if !try!(fs::metadata(entry.path())).is_dir() {
                println!("Entry: {:?}", entry.path());
//            }
        }
        
        return Ok(FileManager{
            root: dir.as_ref().to_path_buf(),
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
        let mut fm = super::FileManager::open_or_create("/tmp/filemanager")
            .expect("FileManager::open");
        assert_eq!(None, fm.log());
        assert_eq!("/tmp/filemanager/log_0", fm.new_log_file());
        assert_eq!("/tmp/filemanager/log_1", fm.new_log_file());
        assert_eq!("/tmp/filemanager/log_1", fm.log().unwrap());
    }
}

