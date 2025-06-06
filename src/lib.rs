mod index;

pub mod kv_db {
    use std::{
        fs::{OpenOptions},
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

            let mut db = Database { path, index };
            db.init();

            db
        }


        fn init(&mut self) {
            println!("Initializing index.");
            self.index.init(&self.path);
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
                let f = OpenOptions::new().read(true).open(self.path).unwrap();
                let mut buf: Vec<u8> = vec![0; *size];

                let offset: u64 = <usize as TryInto<u64>>::try_into(*offset).unwrap();
                
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
}

