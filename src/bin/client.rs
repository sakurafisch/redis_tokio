use mini_redis::client;
use bytes::Bytes;
use tokio::sync::{ mpsc, oneshot };

type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;

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
    },
}

#[tokio::main]
async fn main() {
    // Create a new channel with a capacity of at most 32
    let (tx, mut rx) = mpsc::channel(32);
    let tx2 = tx.clone();

    let manager = tokio::spawn(async move {
        let mut client = client::connect("localhost:6379").await.unwrap();
        while let Some(cmd) = rx.recv().await {
            use Command::*;
            match cmd {
                Get { key, resp } => {
                    let res = client.get(&key).await;
                    // Ignore errors
                    let _ = resp.send(res);
                }
                Set { key, val, resp } => {
                    let res = client.set(&key, val).await;
                    // Ignore errors
                    let _ = resp.send(res);
                }
            }
        }
    });

    let t1 = tokio::spawn(async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Get {
            key: "hello".to_string(),
            resp: resp_tx,
        };
        // 发送 GET 请求
        tx.send(cmd).await.unwrap();
        // 等待回复
        let res = resp_rx.await;
        println!("GOT = {:?}", res);
    });
    let t2 = tokio::spawn(async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Set {
            key: "foo".to_string(),
            val: "bar".into(),
            resp: resp_tx,
        };
        // 发送 SET 请求
        tx2.send(cmd).await.unwrap();
        // 等待回复
        let res = resp_rx.await;
        println!("GOT = {:?}", res);
    });

    manager.await.unwrap();
    t1.await.unwrap();
    t2.await.unwrap();
}
