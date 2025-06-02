pub mod kv_db {
    use std::{
        fs::{self, OpenOptions},
        io::Write,
        path::Path,
    };

    // todo: implement and pass in an underlying storage engine
    // try to follow open-closed principle via strategy pattern
    //
    // start with a simple csv log with full-file scans for every query
    // build up to indexes, binary file storage, log compaction, etc

    pub struct Database {
        path: &'static Path,
    }

    impl Database {
        pub fn new(path: &'static Path) -> Database {
            Database { path }
        }

        /// Write a key and value into the database
        ///
        /// Note: If a comma is in the saved key or value, there will be problems when you read it
        ///
        /// # Panics
        ///
        /// Panics if key provided is the empty string.
        /// Will panic if it can't find or create the expected file at the given path.
        pub fn set(&self, key: &str, value: &str) {
            assert_ne!(key, "");
            append_to_file(&self.path, &format!("{key},{value}\n")).unwrap();
        }

        /// Given a key, returns an option with the value if present
        ///
        /// Note: currently reads the full file in as a string and iterates over lines
        pub fn get(&self, key: String) -> Option<String> {
            if key == "" {
                return None;
            }

            let contents = fs::read_to_string(&self.path).unwrap_or_else(|_| String::new());

            let mut val: Option<&str> = None;
            for line in contents.lines() {
                match get_csv_row(line) {
                    Some((k, v)) if k == key => {
                        val = Some(v);
                    }
                    Some((_, v)) => {
                        val = Some(v);
                    }
                    None => {
                        continue;
                    }
                }
            }

            if let Some(x) = val {
                Some(String::from(x))
            } else {
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
