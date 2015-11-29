extern crate regex;

use std::fs;
use std::io;
use std::path;
use std::vec::Vec;

pub struct FileManager {
    root: path::PathBuf,
    log_path: Option<String>,
    log_version: usize,

    table_paths: Vec<String>,
    table_count: usize,
}

fn log_file_version(filename: &str) -> Option<usize> {
    let log_re = regex::Regex::new(r".*/log_([0-9]+)").unwrap();
        
    return log_re.captures(filename).and_then(
        |caps| caps.at(1).and_then(
            (|val| return val.parse::<usize>().ok())));
}

fn table_file_version(filename: &str) -> Option<usize> {
    let log_re = regex::Regex::new(r".*/table_([0-9]+)").unwrap();
        
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

        let mut max_log_version : Option<usize> = None;
        let mut max_log_path = None;
        let mut max_table_version : Option<usize> = None;
        let mut table_paths = Vec::new();
        
        for entry in try!(fs::read_dir(dir.as_ref())) {
            let entry = try!(entry);
            println!("Examining: {:?}", entry.path());
            if !try!(fs::metadata(entry.path())).is_dir() {
                match entry.path().to_str().and_then(log_file_version) {
                    Some(v) => {
                        println!("Extracted log: {}", v);
                        if max_log_version.is_none() ||
                            v > max_log_version.unwrap() {
                                max_log_version = Some(v);
                                max_log_path = entry.path().to_str()
                                    .and_then(|s| Some(s.to_string()));
                        }
                    }
                    None => (),
                }
                match entry.path().to_str().and_then(table_file_version) {
                    Some(v) => {
                        println!("Extracted table: {}", v);
                        table_paths.push(
                            entry.path().to_str().unwrap().to_string());
                        if max_table_version.is_none() ||
                            v > max_table_version.unwrap() {
                            max_table_version = Some(v);
                        }
                    }
                    None => (),
                }

            }
        }
        
        return Ok(FileManager{
            root: dir.as_ref().to_path_buf(),
            log_version: max_log_version.map(|v| v + 1).unwrap_or(0),
            log_path: max_log_path,
            table_count: max_table_version.map(|v| v + 1).unwrap_or(0),
            table_paths: table_paths,
        });
    }

    pub fn new_table_file(&mut self) -> String {
        let ct = self.table_count;
        self.table_count += 1;

        let mut buf = self.root.clone();
        buf.push(format!("table_{}", ct));

        let path = buf.to_str().unwrap().to_string();
        self.table_paths.push(path.clone());
        return path;
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
    
    pub fn latest_log(&self) -> Option<String> {
        return self.log_path.clone();
    }

    pub fn table_paths(&self) -> Vec<String> {
        return self.table_paths.clone();
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
            assert_eq!(None, fm.latest_log());
            assert_eq!("/tmp/filemanager/log_0", fm.new_log_file());
            assert_eq!("/tmp/filemanager/log_1", fm.new_log_file());
            assert_eq!("/tmp/filemanager/log_1", fm.latest_log().unwrap());

            let empty : Vec<String> = vec!();
            assert_eq!(empty, fm.table_paths());

            assert_eq!("/tmp/filemanager/table_0", fm.new_table_file());
            assert_eq!("/tmp/filemanager/table_1", fm.new_table_file());
            assert_eq!(vec!["/tmp/filemanager/table_0",
                            "/tmp/filemanager/table_1"], fm.table_paths());

            
            File::create("/tmp/filemanager/log_0").unwrap();
            File::create("/tmp/filemanager/log_1").unwrap();
            File::create("/tmp/filemanager/table_0").unwrap();
            File::create("/tmp/filemanager/table_1").unwrap();
        }

        println!("Recovering...");
        
        {
            let mut fm = super::FileManager::open_or_create("/tmp/filemanager")
                .expect("FileManager::open #2");

            assert_eq!("/tmp/filemanager/log_1", fm.latest_log().unwrap());
            assert_eq!("/tmp/filemanager/log_2", fm.new_log_file());

            assert_eq!(vec!["/tmp/filemanager/table_0",
                            "/tmp/filemanager/table_1"], fm.table_paths());
            assert_eq!("/tmp/filemanager/table_2", fm.new_table_file());


            File::create("/tmp/filemanager/log_2").unwrap();
            File::create("/tmp/filemanager/table_2").unwrap();
        }

        println!("Recovering...");

        {
            let fm = super::FileManager::open_or_create("/tmp/filemanager")
                .expect("FileManager::open #3");
            assert_eq!("/tmp/filemanager/log_2", fm.latest_log().unwrap());
            assert_eq!(vec!["/tmp/filemanager/table_0",
                            "/tmp/filemanager/table_1",
                            "/tmp/filemanager/table_2",], fm.table_paths());
        }

        fs::remove_dir_all("/tmp/filemanager").unwrap();
    }
}

