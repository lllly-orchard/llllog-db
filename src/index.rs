use std::{
    collections::HashMap,
    fs,
    path::Path,
};

#[derive(Debug)]
#[derive(PartialEq)]
pub struct ValueLocation {
    pub offset: u64,
    pub size: u64,
}

pub struct SingleFileIndex {
    file_bytes: u64,
    map: HashMap<String, ValueLocation>,
    kv_seperator: char,
    kv_seperator_len: usize,
    entry_seperator_len: usize,
}

impl SingleFileIndex {
    pub fn new() -> SingleFileIndex {
        let kv_seperator = ',';
        let entry_seperator = '\n';
        let kv_seperator_len = kv_seperator.len_utf8();
        let entry_seperator_len = entry_seperator.len_utf8();
        SingleFileIndex {
            file_bytes: 0,
            map: HashMap::new(),
            kv_seperator,
            kv_seperator_len,
            entry_seperator_len,
        }
    }

    pub fn set(&mut self, key: &str, entry_size: u64) -> u64 {
        let key_bytes = format!("{key}").len();
        let key_bytes: u64 = TryInto::<u64>::try_into(key_bytes + self.kv_seperator_len).unwrap();

        let value_bytes = entry_size - key_bytes - TryInto::<u64>::try_into(self.entry_seperator_len).unwrap();

        let value_location = ValueLocation {
            offset: self.file_bytes + key_bytes,
            size: value_bytes,
        };

        self.map.insert(String::from(key), value_location);

        self.file_bytes += entry_size;

        self.file_bytes
    }

    pub fn get(&self, key: &str) -> Option<&ValueLocation> {
        self.map.get(key)
    }

    /// Initializes the index from a provided file
    ///
    /// # TODO
    ///
    /// 1. Read file one line at a time rather than the whole file as a string
    pub fn init(&mut self, path: &Path) {
        let contents = fs::read_to_string(path).unwrap_or_else(|_| String::new());

        let entry_seperator_bytes = self.entry_seperator_len;
        for line in contents.lines() {
            let (k, _) = match self.parse_csv_row(line) {
                Some((k, v)) => {
                    (k, v)
                }
                _ => {
                    panic!("Unable to parse data file.");
                }
            };

            let size: u64 = TryInto::try_into(line.len() + entry_seperator_bytes).unwrap(); // the +1 is for the newline, which .lines() drops
            self.set(k, size);
        }
    }

    /// Splits a &str on "," and returns the values as a (x, y) tuple option,
    /// or None if two values are not found for the line
    ///
    /// # TODO
    ///
    /// 1. Respect escapes ("\,")
    /// 2. Allow different characters to split on
    fn parse_csv_row<'a>(&self, line: &'a str) -> Option<(&'a str, &'a str)> {
        let mut iter = line.split(self.kv_seperator);
        let k = iter.next();
        let v = iter.next();

        match (k, v) {
            (None, _) => None,
            (Some(_), None) => None,
            (Some(x), Some(y)) => Some((x, y)),
        }
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

        let file_size_new: u64 = 0;
        let file_size_after_k1 = index.set(k1_key, k1_size);

        let result = index.get(k1_key).unwrap();

        let comma_len: u64 = TryInto::try_into(",".len()).unwrap();
        let newline_len: u64 = TryInto::try_into("\n".len()).unwrap();
        let key_len: u64 = TryInto::try_into(k1_key.len()).unwrap();
        let expected_val = ValueLocation {
            offset: file_size_new + key_len + comma_len,
            size: k1_size - key_len - comma_len - newline_len,
        };
        assert_eq!(result.offset, expected_val.offset);
        assert_eq!(result.size, expected_val.size);

        let k2_size = 12;
        let k2_key = "key2";
        index.set(k2_key, k2_size);

        let result = index.get(k2_key).unwrap();

        let key_len: u64 = TryInto::try_into(k2_key.len()).unwrap();
        let expected_val = ValueLocation {
            offset: file_size_after_k1 + key_len + comma_len,
            size: k2_size - key_len - comma_len - newline_len,
        };
        assert_eq!(result.offset, expected_val.offset);
        assert_eq!(result.size, expected_val.size);
    }
}

/// An index spanning multiple files.
///
/// Under the hood, this index maintains an ordered list of single file indexes,
/// sorted with the most recent file first.
///
/// A `get` call will call `get` on each index in this list in turn and return the
/// first found result (path, offset, length) or None if no index contains the key
///
/// A `set` call will append the k-v pair to the newest file if it is below capacity,
/// or a new file with a new index if it is not
#[allow(dead_code)]
struct MultiFileIndex {
    file_indexes: Vec<SingleFileIndex>,
}


#[allow(dead_code)]
impl MultiFileIndex {
    pub fn new() -> Self {
        MultiFileIndex {
            file_indexes: Vec::new()
        }
    }

    /// Initializes the index using the specified config file to determine datafile load order
    ///
    /// # TODO
    ///
    /// Get datafile load order from config file
    ///
    /// Load each datafile into its own SingleFileIndex
    pub fn init(&mut self, _: &Path) {
        unimplemented!(); }

    /// Adds an index for a file to self.file_indexes
    ///
    /// # TODO
    ///
    /// Place index in correct position relative to others, potentially by filename
    /// convention
    ///
    /// Write updated file order into config file
    pub fn add_file(&mut self, file_path: &Path) {
        let mut new_index = SingleFileIndex::new();
        new_index.init(file_path);

        // TODO: place in appropriate spot in collection
        self.file_indexes.push(new_index);
    }
    
    /// Looks through each index in sorted order and returns Some(x)
    /// if the key is found, or None if it is not found in any index
    ///
    /// # TODO
    ///
    /// Update to return the file name/path/etc to the caller
    /// along with (offset, size) to allow the caller to locate the value
    pub fn get(&self, key: &str) -> Option<&ValueLocation> {
        for index in self.file_indexes.iter() {
            match index.get(key) {
                Some(str) => {
                    return Some(str);
                },
                None => {},
            }
        }

        None
    }

    // Adds the key and its size to the appropriate index based on file path
    //
    // TODO: implement it, making sure to locate the correct index
    pub fn set(&mut self, key: &str, size: usize, file_path: &Path) {
        dbg!(key, size, file_path);
    }
}


