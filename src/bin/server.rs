use std::{collections::HashMap, sync::{Arc, RwLock}};

use bytes::Bytes;
use mini_redis::{Connection, Frame, Command, cmd::{Set, Get}};
use tokio::net::{TcpListener, TcpStream};

type Db = Arc<RwLock<HashMap<String, Bytes>>>;

#[tokio::main]
async fn main() {
	let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

	let db: Db = Arc::new(RwLock::new(HashMap::new()));

	loop {
		let (stream, _) = listener.accept().await.unwrap();
		let db = db.clone();
		tokio::spawn(async move {
			process(stream, db).await;
		});
	}

}

async fn process(stream: TcpStream, db: Db) {
	let mut conn = Connection::new(stream);

	while let Some(frame) = conn.read_frame().await.unwrap() {
		let response = match Command::from_frame(frame).unwrap() {
			Command::Set(cmd) => process_set(cmd, &db),
			Command::Get(cmd) => process_get(cmd, &db),
			cmd => panic!("unimplemented {:?}", cmd),
		};
		conn.write_frame(&response).await.unwrap();
	}
}

fn process_set(cmd: Set, db: &Db) -> Frame {
	db.write().unwrap().insert(cmd.key().to_string(), cmd.value().clone());
	Frame::Simple("OK".to_string())
}

fn process_get(cmd: Get, db: &Db) -> Frame {
	if let Some(value) = db.read().unwrap().get(cmd.key()) {
		Frame::Bulk(value.clone())
	} else {
		Frame::Null
	}
}