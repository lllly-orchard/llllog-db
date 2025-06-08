mod index;

pub mod kv_db {
    use std::{
        fs::{OpenOptions, File},
        io::Write,
        os::unix::fs::FileExt,
        path::Path,
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

            let db = Database { path, index };

            db.init()
        }


        fn init(mut self) -> Self {
            println!("Initializing index.");
            self.index.init(&self.path);

            self
        }

        /// Write a key and value into the database
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
        /// # Performance
        ///
        /// Runs in linear time based on size of *returned value*
        ///
        /// Index lookup is O(1), seeking an offset in the file is O(1)
        /// and reading the length of the content is O(n), where n is content size
        pub fn get(&self, key: String) -> Option<String> {
            let val_option = self.index.get(&key);
            if let Some((offset, size)) = val_option {
                let f = OpenOptions::new().read(true).open(self.path).unwrap();

                let offset: u64 = TryInto::try_into(*offset).unwrap();
                let content = read_exact_str_at(&f, offset, *size).unwrap();

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

    /// Reads exactly `size` bytes from `file` with `offset` bytes offset from start
    ///
    /// # Panics
    ///
    /// This will panic any time the underlying `read_exact_at` call would return an error
    fn read_exact_str_at(file: &File, offset: u64, size: usize) -> Result<String, std::string::FromUtf8Error> {
                let mut buf: Vec<u8> = vec![0; size];
                file.read_exact_at(buf.as_mut_slice(), offset).unwrap();

                String::from_utf8(buf)
    }
}

