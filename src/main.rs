fn main() {
    let my_db = kv_db::Database { id: 42 };

    my_db.print_id();

    my_db.set(String::from("key1"), String::from("val1"));
    let value = my_db.get(String::from("key1"));

    println!("Value retrieved: {:?}", value);
}

pub mod kv_db {
    // todo: implement and pass in an underlying storage engine
    // try to follow open-closed principle via strategy pattern
    //
    // start with a simple csv log with full-file scans for every query
    // build up to indexes, binary file storage, log compaction, etc

    pub struct Database {
        pub id: u32,
    }

    impl Database {
        pub fn print_id(&self) {
            println!("ID: {}", self.id);
        }

        pub fn set(&self, key: String, value: String) {
            println!("Storing value \"{}\" under key \"{}\"", value, key);
        }

        pub fn get(&self, key: String) -> Option<String> {
            println!("Attempting to retrieve value with key \"{}\"", key);

            None
        }
    }
}
