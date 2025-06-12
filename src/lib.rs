mod index;

pub mod kv_db {
    use std::{
        fs::OpenOptions,
        io::{Read, Seek, SeekFrom, Write},
        path::Path, string::FromUtf8Error,
    };
    use crate::index;

    /// Log-Structured Key-Value Database
    ///
    /// # Warnings
    ///
    /// Will not initialize properly if the key or value contain newline characters
    /// Will not initialize properly if the key contains a comma character
    ///
    /// # TODO
    ///
    /// Escape commas and newlines in keys and values and respect those escapes
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

            self.index.set(key, TryInto::try_into(size).unwrap());
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
            if let Some(index::ValueLocation {offset, size}) = val_option {
                let mut f = OpenOptions::new().read(true).open(self.path).unwrap();

                let offset: u64 = TryInto::try_into(*offset).unwrap();
                let content = read_exact_str_at(&mut f, offset, *size).unwrap();

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

    /// Reads exactly `size` bytes from `stream` with `offset` bytes offset from start
    ///
    /// # Panics
    ///
    /// This will panic any time the underlying `seek` or `read` calls return an error
    fn read_exact_str_at<R: Read + Seek>(stream: &mut R, offset: u64, size: u64) -> Result<String, FromUtf8Error> {
        let mut buf: Vec<u8> = vec![0; TryInto::try_into(size).unwrap()];
        stream.seek(SeekFrom::Start(offset)).unwrap();
        stream.read(&mut buf).unwrap();
        String::from_utf8(buf)
    }


    #[cfg(test)]
    mod test {
        use std::io::Write;

        use crate::kv_db::read_exact_str_at;

        #[test]
        fn read_exact_str_at_reads_the_expected_content() {
            use std::io::Cursor;
            let mut buff = Cursor::new(vec![0; 15]);

            for i in 0..15 {
                buff.write(std::format!("{i}").as_bytes()).unwrap();
            }


            // From beginning
            assert_eq!(read_exact_str_at(&mut buff, 0, 5).unwrap(), "01234");

            // From middle
            assert_eq!(read_exact_str_at(&mut buff, 5, 5).unwrap(), "56789");

            // Up to end and slightly beyond
            assert_eq!(read_exact_str_at(&mut buff, 10, 12).unwrap(), "1011121314\0\0");
        }
    }
}

