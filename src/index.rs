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

