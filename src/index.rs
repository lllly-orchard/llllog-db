use std::{
    collections::HashMap,
    fs,
    path::Path,
};

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

    /// Initializes the index from a provided file
    ///
    /// # TODO
    ///
    /// 1. Read file one line at a time rather than the whole file as a string
    pub fn init(&mut self, path: &Path) {
        let contents = fs::read_to_string(path).unwrap_or_else(|_| String::new());

        for line in contents.lines() {
            let (k, _) = match SingleFileIndex::parse_csv_row(line) {
                Some((k, v)) => {
                    (k, v)
                }
                _ => {
                    panic!("Unable to parse data file.");
                }
            };

            let size = line.len() + 1; // the +1 is for the newline, which .lines() drops
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
    fn parse_csv_row(line: &str) -> Option<(&str, &str)> {
        let mut iter = line.split(",");
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
struct MultiFileIndex {
    file_indexes: Vec<SingleFileIndex>,

}

impl MultiFileIndex {
    /// Adds an index for a file to self.file_indexes
    ///
    /// TODO: place index in correct position relative to others, potentially by filename
    /// convention
    pub fn add_file(&mut self, file_path: &Path) {
        let mut new_index = SingleFileIndex::new();
        new_index.init(file_path);

        // TODO: place in appropriate spot in collection
        self.file_indexes.push(new_index);
    }
    
    /// Looks through each index in sorted order and returns Some(x)
    /// if the key is found, or None if it is not found in any index
    ///
    /// TODO: Update to return the file name/path/etc to the caller
    /// along with (offset, size) to allow the caller to locate the value
    pub fn get(&self, key: &str) -> Option<&(usize, usize)> {
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
    pub fn set(&mut self, key: &str, size: usize, file_path: &Path) { }
}


