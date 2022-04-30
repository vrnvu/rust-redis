
use core::time;
use std::{thread};

use mini_redis::{client};
use tokio::sync::{mpsc::{self, error::SendError}, oneshot::{self}};

use bytes::Bytes;

type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug)]
enum Command {
    Get {
        key: String,
	resp: Responder<Option<Bytes>>,
    },
    Set {
        key: String,
        val: Bytes,
	resp: Responder<()>,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
	let (tx, mut rx) = mpsc::channel(32);

	let tx2 = tx.clone();

	let t1 = tokio::spawn(async move {
		let (resp_tx, resp_rx) = oneshot::channel();
		let cmd = Command::Get { 
			key: "hello".to_string(),
			resp: resp_tx,
		};

		// i want to get world so I sleep the thread
		thread::sleep(time::Duration::from_millis(10));
		tx.send(cmd).await?;

		let res = resp_rx.await;
		println!("t1 got {:?}", res);
		Ok::<(), SendError<Command>>(()) 
	});

	let t2 = tokio::spawn(async move {
		let (resp_tx, resp_rx) = oneshot::channel();
		let cmd = Command::Set{
			key: "hello".to_string(),
			val: "world".into(),
			resp: resp_tx,
		};

		tx2.send(cmd).await?;

		let res = resp_rx.await;
		println!("t2 got {:?}", res);

		Ok::<(), SendError<Command>>(()) 
	});

	let manager = tokio::spawn(async move {
		let mut client = client::connect("127.0.0.1:6379").await.unwrap();

		while let Some(cmd) = rx.recv().await {
			match cmd {
				Command::Get { key, resp } => {
					let res = client.get(&key).await; 
					let _ = resp.send(res);
				},
				Command::Set { key, val, resp } => {
					let res = client.set(&key, val).await;
					let _ = resp.send(res);
				},
			}
		}
	});

	t1.await??;
	t2.await??;
	manager.await?;

	Ok(())
}