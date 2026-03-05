use crate::{configs::OmniPaxosKVConfig, database::Database, network::Network};
use chrono::Utc;
use log::*;
use omnipaxos::{
    messages::Message,
    util::{LogEntry, NodeId},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_kv::common::{kv::*, messages::*, utils::Timestamp};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::{fs::File, io::Write, time::Duration};

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);

pub struct OmniPaxosServer {
    id: NodeId,
    database: Database,
    network: Network,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    config: OmniPaxosKVConfig,
    peers: Vec<NodeId>,
}

impl OmniPaxosServer {
    pub async fn new(config: OmniPaxosKVConfig) -> Self {
        // Initialize OmniPaxos instance
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        // Waits for client and server network connections to be established
        let network = Network::new(config.clone(), NETWORK_BATCH_SIZE).await;
        OmniPaxosServer {
            id: config.local.server_id,
            database: Database::new(),
            network,
            omnipaxos,
            current_decided_idx: 0,
            omnipaxos_msg_buffer,
            peers: config.get_peers(config.local.server_id),
            config,
        }
    }

    pub async fn run(&mut self) {
        // Save config to output file
        self.save_output().expect("Failed to write to file");
        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election at first and instead force a specific initial leader
        self.establish_initial_leader(&mut cluster_msg_buf, &mut client_msg_buf)
            .await;
        // Main event loop with leader election
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        loop {
            tokio::select! {
                _ = election_interval.tick() => {
                    self.omnipaxos.tick();
                    self.send_outgoing_msgs();
                },
                _ = self.network.cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
                },
                _ = self.network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(&mut client_msg_buf).await;
                },
            }
        }
    }

    // Ensures cluster is connected and initial leader is promoted before returning.
    // Once the leader is established it chooses a synchronization point which the
    // followers relay to their clients to begin the experiment.
    async fn establish_initial_leader(
        &mut self,
        cluster_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>,
        client_msg_buffer: &mut Vec<(ClientId, ClientMessage)>,
    ) {
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        loop {
            tokio::select! {
                _ = leader_takeover_interval.tick(), if self.config.cluster.initial_leader == self.id => {
                    if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader(){
                        if curr_leader == self.id && is_accept_phase {
                            info!("{}: Leader fully initialized", self.id);
                            let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
                            self.send_cluster_start_signals(experiment_sync_start);
                            self.send_client_start_signals(experiment_sync_start);
                            break;
                        }
                    }
                    info!("{}: Attempting to take leadership", self.id);
                    self.omnipaxos.try_become_leader();
                    self.send_outgoing_msgs();
                },
                _ = self.network.cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE) => {
                    let recv_start = self.handle_cluster_messages(cluster_msg_buffer).await;
                    if recv_start {
                        break;
                    }
                },
                _ = self.network.client_messages.recv_many(client_msg_buffer, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(client_msg_buffer).await;
                },
            }
        }
    }

    // Checks whether there has been any new decided entries in its log.
    // If there are new entries, then read those new entries
    // And apply them locally .. ?
    fn handle_decided_entries(&mut self) {
        // TODO: Can use a read_raw here to avoid allocation
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_idx) // Reads ONLY new decided entries
                .unwrap();
            self.current_decided_idx = new_decided_idx; // Update existing decided_idx
            debug!("Decided {new_decided_idx}");
            let decided_commands = decided_entries.into_iter() // Owned iterator, consumes the decided_entries vectors
                .filter_map(|e| match e { // if e == LogEntry::Decided(Command{id: 7, ...}) then bind cmd = Command{id: 7, ...} and keep it in new array
                    LogEntry::Decided(cmd) => Some(cmd),
                    _ => unreachable!(),
                })
                .collect();
            self.update_database_and_respond(decided_commands);
        }
    }

    // Applies decided entries (log commands) to local state machine
    // After decided entry(s) have been applied locally, replica checks if it was the original recipient of the request
    // If so, prepares and sends back indication to original client. (Since interface is <req> <indication> then it is only right that the same replica who received the request sends back the indication
    fn update_database_and_respond(&mut self, commands: Vec<Command>) {
        // TODO: batching responses possible here (batch at handle_cluster_messages)
        for command in commands {
            let read = self.database.handle_command(command.kv_cmd); // Applies the command and returns either None or some get-result.
            if command.coordinator_id == self.id { // Original receiver prepares indication back to client
                let response = match read {
                    Some(read_result) => ServerMessage::Read(command.id, read_result), // if read happened in handle_command
                    None => ServerMessage::Write(command.id), // if Write or Delete happened (it returns None at end of handle_command in that case)
                };
                // In the above pattern matching we bind the response as to be either a read or write.
                // If the client requested a read operation, then response (i.e., the read) will be a tuple ServerMessage::Read(command.id, read_result)
                // And then sent back to the client.
                // If it was a Write or Delete, then response will be evaluated to a ServerMessage::Write(command.id) and sent to client together with the id of the command.
                self.network.send_to_client(command.client_id, response); // Sends indication back to original client.
            }
        }
    }

    fn send_outgoing_msgs(&mut self) {
        self.omnipaxos
            .take_outgoing_messages(&mut self.omnipaxos_msg_buffer);
        for msg in self.omnipaxos_msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send_to_cluster(to, cluster_msg);
        }
    }

    // An incoming GET, PUT, ETC FROM CLIENT
    // This is where the server processes the incoming message from the client.
    // It checks to see if the incoming message is an Append, if so, then wraps the command_id and kv_command (GET, ...)
    // and calls append_to_log with the fields from the message.
    // The append_to_log wraps an omnipaxos message and calls omnipaxos on itself to append to its local log.
    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        for (from, message) in messages.drain(..) {
            match message {
                ClientMessage::Append(command_id, kv_command) => { // If it is a client message, then call append_to_log
                    self.append_to_log(from, command_id, kv_command)
                }
            }
        }
        self.send_outgoing_msgs();
    }

    // Triggered when a cluster of messages enters the node from the network.
    // Pattern matches on what kind of message is arrived
    async fn handle_cluster_messages(&mut self, messages: &mut Vec<(NodeId, ClusterMessage)>, ) -> bool {
        let mut received_start_signal = false;
        for (from, message) in messages.drain(..) {
            trace!("{}: Received {message:?}", self.id);
            match message { // What kind of message did we receive through the network?
                ClusterMessage::OmniPaxosMessage(m) => { // If it's an omnipaxos message
                    self.omnipaxos.handle_incoming(m); // Pass it to consensus engine (omni paxos). This advances the prepare/accept/accepted/decide internally in omnipaxos
                    self.handle_decided_entries(); // Check whether that caused new decisions.
                }
                ClusterMessage::LeaderStartSignal(start_time) => {
                    debug!("Received start message from peer {from}");
                    received_start_signal = true;
                    self.send_client_start_signals(start_time);
                }
            }
        }
        self.send_outgoing_msgs();
        received_start_signal
    }

    // Wraps an API call to the omniPaxos API to append a log to the entry.
    // Called during handle_client_messages.
    fn append_to_log(&mut self, from: ClientId, command_id: CommandId, kv_command: KVCommand) {
        let command = Command {
            client_id: from,
            coordinator_id: self.id, // Adds its own ID to the command (for further use when indication event happens)
            id: command_id,
            kv_cmd: kv_command,
        };
        self.omnipaxos
            .append(command)
            .expect("Append to Omnipaxos log failed");
    }

    fn send_cluster_start_signals(&mut self, start_time: Timestamp) {
        for peer in &self.peers {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time);
            self.network.send_to_cluster(*peer, msg);
        }
    }

    fn send_client_start_signals(&mut self, start_time: Timestamp) {
        for client_id in 1..self.config.local.num_clients as ClientId + 1 {
            debug!("Sending start message to client {client_id}");
            let msg = ServerMessage::StartSignal(start_time);
            self.network.send_to_client(client_id, msg);
        }
    }

    fn save_output(&mut self) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&self.config)?;
        let mut output_file = File::create(&self.config.local.output_filepath)?;
        output_file.write_all(config_json.as_bytes())?;
        output_file.flush()?;
        Ok(())
    }
}
