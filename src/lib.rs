pub mod kv_db {
    use std::{
        fs::{self, OpenOptions}, io::{Seek, SeekFrom, Write}, os::unix::fs::FileExt, panic, path::Path
    };

    use crate::index;

    // todo: implement and pass in an underlying storage engine
    // try to follow open-closed principle via strategy pattern
    //
    // start with a simple csv log with full-file scans for every query
    // build up to indexes, binary file storage, log compaction, etc

    pub struct Database {
        path: &'static Path,
        index: index::SingleFileIndex,
    }

    impl Database {
        pub fn build(path: &'static Path) -> Database {
            let index = index::SingleFileIndex::new();

            let mut db = Database { path, index };
            db.init();

            db
        }


        fn init(&mut self) {
            println!("Initializing database index.");
            let contents = fs::read_to_string(&self.path).unwrap_or_else(|_| String::new());

            for line in contents.lines() {
                let (k, _) = match get_csv_row(line) {
                    Some((k, v)) => {
                        (k, v)
                    }
                    _ => {
                        panic!("Unable to parse data file.");
                    }
                };

                let size = line.len() + 1; // the +1 is for the newline, which .lines() drops
                self.index.set(k, size);
            }
        }

        /// Write a key and value into the database
        ///
        /// Note: If a comma is in the saved key or value, there will be problems when you read it
        ///
        /// # Panics
        ///
        /// Panics if key provided is the empty string.
        /// Will panic if it can't find or create the expected file at the given path.
        pub fn set(&mut self, key: &str, value: &str) {
            assert_ne!(key, "");
            let size = append_to_file(&self.path, &format!("{key},{value}\n")).unwrap();

            self.index.set(key, size);
        }

        /// Given a key, returns an option with the value if present
        ///
        /// Note: currently reads the full file in as a string and iterates over lines
        pub fn get(&self, key: String) -> Option<String> {
            let val_option = self.index.get(&key);
            if let Some((offset, size)) = val_option {
                let mut f = OpenOptions::new().read(true).open(self.path).unwrap();
                let mut buf: Vec<u8> = vec![0; *size];

                let offset: u64 = <usize as TryInto<u64>>::try_into(*offset).unwrap();
                f.seek(SeekFrom::Start(offset)).unwrap();
                
                f.read_exact_at(buf.as_mut_slice(), offset).unwrap();

                let content = String::from_utf8(buf).unwrap();

                Some(content)
            }
            else {
                None
            }
        }
    }

    /// Appends contents to a file.
    ///
    /// # Panics
    ///
    /// Will panic if it fails to open a file.
    fn append_to_file(path: &Path, contents: &str) -> Result<usize, std::io::Error> {
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .unwrap();

        file.write(contents.as_bytes())
    }

    fn get_csv_row(line: &str) -> Option<(&str, &str)> {
        let mut iter = line.split(",");
        let k = iter.next();
        let v = iter.next();

        match (k, v) {
            (None, _) => {
                return None;
            }
            (Some(_), None) => {
                return None;
            }
            (Some(x), Some(y)) => {
                return Some((x, y));
            }
        }
    }
}

mod index {
    use std::collections::HashMap;

    pub struct SingleFileIndex {
        file_bytes: usize,
        map: HashMap<String, (usize, usize)>,
    }

    impl SingleFileIndex {
        pub fn new() -> SingleFileIndex {
            SingleFileIndex {
                file_bytes: 0,
                map: HashMap::new(),
            }
        }

        pub fn set(&mut self, key: &str, entry_size: usize) -> usize {
            let key_bytes = format!("{key},").len();
            let value_bytes = entry_size - key_bytes - 1;

            self.map.insert(String::from(key), (self.file_bytes + key_bytes, value_bytes));

            self.file_bytes += entry_size;

            self.file_bytes
        }

        pub fn get(&self, key: &str) -> Option<&(usize, usize)> {
            self.map.get(key)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn it_builds() {
            SingleFileIndex::new();
        }

        #[test]
        fn get_returns_none_when_empty() {
            let index = SingleFileIndex::new();
            assert_eq!(index.get("key"), None);
        }

        #[test]
        fn set_returns_total_bytes_written() {
            let mut index = SingleFileIndex::new();

            let k1_size = 11;
            let size = index.set("k1", k1_size);
            assert_eq!(size, 11);

            let k2_size = 12;
            let size = index.set("k2", k2_size);
            assert_eq!(size, k1_size + k2_size);
        }

        #[test]
        fn get_returns_correct_offset_and_value_size() {
            let mut index = SingleFileIndex::new();

            let k1_size = 11;
            let k1_key = "key1";

            let file_size_new = 0;
            let file_size_after_k1 = index.set(k1_key, k1_size);

            let result = index.get(k1_key).unwrap();

            let comma_len = ",".len();
            let newline_len = "\n".len();
            let key_len = k1_key.len();
            assert_eq!(*result, (file_size_new + key_len + comma_len, k1_size - key_len - comma_len - newline_len));

            let k2_size = 12;
            let k2_key = "key2";
            index.set(k2_key, k2_size);

            let result = index.get(k2_key).unwrap();

            let key_len = k2_key.len();
            assert_eq!(*result, (file_size_after_k1 + key_len + comma_len, k2_size - key_len - comma_len - newline_len));
        }
    }
}



