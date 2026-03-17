use env_logger;
use futures::{SinkExt, StreamExt};
use omnipaxos_kv::common::{
    kv::{KVCommand, CommandId}, // Explicitly import the type
    messages::{ClientMessage, RegistrationMessage, ServerMessage},
    utils::{frame_registration_connection, frame_clients_connection},
};
use std::time::Instant;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    env_logger::init();

    let server_addr = "127.0.0.1:8001";
    let stream = TcpStream::connect(server_addr).await.expect("Failed to connect to proxy server");
    stream.set_nodelay(true).unwrap();

    let mut reg_conn = frame_registration_connection(stream);
    reg_conn.send(RegistrationMessage::ClientRegister).await.unwrap();
    println!("Connected to server. Waiting for StartSignal...");

    let stream = reg_conn.into_inner().into_inner();
    let (mut reader, mut writer) = frame_clients_connection(stream);

    while let Some(Ok(msg)) = reader.next().await {
        if let ServerMessage::StartSignal(_) = msg {
            println!("StartSignal received! Commencing load generation...");
            break;
        }
    }

    let num_requests = 1000;
    let mut latencies = Vec::with_capacity(num_requests);
    let start_time = Instant::now();

    for i in 0..num_requests {
        let cmd = KVCommand::Put(format!("key_{}", i), format!("val_{}", i));
        let req_start = Instant::now();

        // Wrap the index in the CommandId type expected by your library
        let cmd_id = CommandId(i as u64);

        writer.send(ClientMessage::Append(cmd_id, cmd)).await.unwrap();

        while let Some(Ok(msg)) = reader.next().await {
            match msg {
                // Access the inner value using .0 to compare the IDs
                ServerMessage::Write(recv_id) if recv_id.0 == cmd_id.0 => {
                    latencies.push(req_start.elapsed().as_micros());
                    break;
                }
                ServerMessage::Read(recv_id, _) if recv_id.0 == cmd_id.0 => {
                    latencies.push(req_start.elapsed().as_micros());
                    break;
                }
                _ => {}
            }
        }
    }

    let total_time = start_time.elapsed();
    latencies.sort();

    let avg_latency = latencies.iter().sum::<u128>() as f64 / latencies.len() as f64 / 1000.0;
    let p99_index = (latencies.len() as f64 * 0.99) as usize;
    let p99_latency = latencies[p99_index.min(latencies.len() - 1)] as f64 / 1000.0;
    let throughput = (num_requests as f64 / total_time.as_secs_f64()) as u64;

    println!("\n--- Client Results ---");
    println!("Throughput:       {} req/sec", throughput);
    println!("Avg Latency:      {:.2} ms", avg_latency);
    println!("P99 Latency:      {:.2} ms", p99_latency);
    println!("----------------------");
}