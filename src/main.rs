use bytes::Bytes;
use mini_redis::{Connection, Frame};
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

pub struct ShardedDb {
    db: Arc<Vec<Mutex<HashMap<String, Bytes>>>>,
}

impl ShardedDb {
    pub fn new(num_shards: usize) -> Self {
        let mut db = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            db.push(Mutex::new(HashMap::new()));
        }

        ShardedDb { db: Arc::new(db) }
    }

    pub fn insert(&self, key: String, value: Bytes) {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);

        // feels bad to cast u64 as usize here, but we're modulo-ing...
        let mut shard = self.db[hasher.finish() as usize % self.db.len()]
            .lock()
            .unwrap();
        shard.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<Bytes> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);

        let shard = self.db[hasher.finish() as usize % self.db.len()]
            .lock()
            .unwrap();
        // I wonder if this is still zero-copy clone()?
        return shard.get(&key).cloned();
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("Listening...");

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let db = ShardedDb::new(10);
        println!("accepted...");
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

async fn process(socket: TcpStream, db: ShardedDb) {
    use mini_redis::Command::{self, Get, Set};

    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                if let Some(value) = db.get(cmd.key().to_string()) {
                    Frame::Bulk(value.clone())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented {:?}", cmd),
        };

        connection.write_frame(&response).await.unwrap();
    }
}
