extern crate regex;

use std::fs;
use std::io;
use std::path;

pub struct FileManager {
    root: path::PathBuf,
    log_path: Option<String>,
    log_version: usize,
}

fn log_file_version(filename: &str) -> Option<usize> {
    let log_re = regex::Regex::new(r".*/log_([0-9]+)").unwrap();
        
    return log_re.captures(filename).and_then(
        |caps| caps.at(1).and_then(
            (|val| return val.parse::<usize>().ok())));

}

impl FileManager {
    pub fn open_or_create<P: AsRef<path::Path>>(dir: P) -> io::Result<FileManager> {
        let md = fs::metadata(dir.as_ref());

        match md {
            Err(err) => {
                if err.kind() != io::ErrorKind::NotFound {
                    return Err(err);
                } else {
                    println!("Creating new FileManager dir");
                    try!(fs::create_dir(dir.as_ref()));
                }
            },
            Ok(md) => {
                println!("Recovering old FileManager dir");
                if !md.is_dir() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("'{:?}' is not a directory.", dir.as_ref())));
                }
            }
        }

        let mut max_version : Option<usize> = None;
        let mut max_path = None;
        
        for entry in try!(fs::read_dir(dir.as_ref())) {
            let entry = try!(entry);
            println!("Examining: {:?}", entry.path());
            if !try!(fs::metadata(entry.path())).is_dir() {
                match entry.path().to_str().and_then(log_file_version) {
                    Some(v) => {
                        println!("Extracted: {}", v);
                        if max_version.is_none() || v > max_version.unwrap() {
                            max_version = Some(v);
                            max_path = entry.path().to_str()
                                .and_then(|s| Some(s.to_string()));
                        }
                    }
                    None => continue,
                }
            }
        }
        
        return Ok(FileManager{
            root: dir.as_ref().to_path_buf(),
            log_version: max_version.map(|v| v + 1).unwrap_or(0),
            log_path: max_path,
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
    use std::fs;
    use std::fs::File;
    use std::io;

    fn accept_not_found(err: io::Error) -> io::Result<()> {
        if err.kind() == io::ErrorKind::NotFound {
            return Ok(());
        }
        return Err(err);
    }
    
    #[test]
    fn basic() {
        fs::remove_dir_all("/tmp/filemanager").or_else(accept_not_found).unwrap();
        {
            let mut fm = super::FileManager::open_or_create("/tmp/filemanager")
                .expect("FileManager::open #1");
            assert_eq!(None, fm.log());
            assert_eq!("/tmp/filemanager/log_0", fm.new_log_file());
            assert_eq!("/tmp/filemanager/log_1", fm.new_log_file());
            assert_eq!("/tmp/filemanager/log_1", fm.log().unwrap());

            File::create("/tmp/filemanager/log_0").unwrap();
            File::create("/tmp/filemanager/log_1").unwrap();
        }

        println!("Recovering...");
        
        {
            let mut fm = super::FileManager::open_or_create("/tmp/filemanager")
                .expect("FileManager::open #2");

            assert_eq!("/tmp/filemanager/log_1", fm.log().unwrap());
            assert_eq!("/tmp/filemanager/log_2", fm.new_log_file());
            File::create("/tmp/filemanager/log_2").unwrap();
        }

        println!("Recovering...");

        {
            let fm = super::FileManager::open_or_create("/tmp/filemanager")
                .expect("FileManager::open #3");
            assert_eq!("/tmp/filemanager/log_2", fm.log().unwrap());
        }

        fs::remove_dir_all("/tmp/filemanager").unwrap();
    }
}

