use llllog_db::kv_db::Database;
use std::path::Path;

fn main() {
    let mut my_db = Database::build(Path::new("db_file.csv"));

    my_db.set("key1", "val1.4");
    my_db.set("key2", "val2.3");
    my_db.set("key3", "val3.0");

    get_and_print(&my_db, "key1");
    get_and_print(&my_db, "key2");
    get_and_print(&my_db, "key3");
    get_and_print(&my_db, "bad_key");
}

fn get_and_print(db: &Database, key: &str) {
    let value = db.get(String::from(key));

    match value {
        None => {
            println!("{key} => No value retrieved")
        }
        Some(x) => {
            println!("{key} => {:?}", x);
        }
    }
}
