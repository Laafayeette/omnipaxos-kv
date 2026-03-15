use crate::{configs::OmniPaxosKVConfig, server::OmniPaxosServer};
use env_logger;
use omnipaxos_kv::clock::ClockSimulator;
use std::time::Duration;

mod configs;
mod database;
mod network;
mod server;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let server_config = match OmniPaxosKVConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    let clock = ClockSimulator::new(0.0, 100, Duration::from_secs(1))
        .expect("Invalid clock parameters");
    let mut server = OmniPaxosServer::new(server_config, clock).await;
    server.run().await;
}
