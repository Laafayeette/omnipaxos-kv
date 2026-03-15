use crate::{configs::OmniPaxosKVConfig, database::Database, network::Network};
use omnipaxos_kv::clock::ClockSimulator;
use chrono::Utc;
use log::*;
use omnipaxos::{
    OmniPaxos, OmniPaxosConfig, messages::Message, util::{LogEntry, NodeId}
};
use omnipaxos_kv::common::{kv::*, messages::*, utils::Timestamp};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::pin::Pin;
use tokio::time::{sleep, Instant, Sleep};
use serde::Serialize;
use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    io::Write,
    time::Duration,
};

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);

const OWD_WINDOW_SIZE: usize = 20;
const OWD_PROBE_INTERVAL: Duration = Duration::from_millis(50);
const OWD_BETA: f64 = 3.0;
const OWD_D_CAP: i64 = 10_000;

struct PeerOWDState {
    window: VecDeque<i64>,
}

pub struct OmniPaxosServer {
    id: NodeId,
    database: Database,
    network: Network,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    config: OmniPaxosKVConfig,
    peers: Vec<NodeId>,

    early_buffer_sleep: Pin<Box<Sleep>>,
    early_buffer_timer_armed: bool,
    clock: ClockSimulator,
    // Receiver-owned sliding windows: one window per sender
    owd_states: HashMap<NodeId, PeerOWDState>,
    // Sender-owned latest estimate returned by each peer's receiver
    deadline_send_array: HashMap<NodeId, i64>,
    // Receiver-owned adaptive D_cap per sender
    receiver_d_cap: HashMap<NodeId, i64>,
}

impl OmniPaxosServer {
    pub async fn new(config: OmniPaxosKVConfig, clock: ClockSimulator) -> Self {
        // Initialize OmniPaxos instance
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        // Waits for client and server network connections to be established
        let network = Network::new(config.clone(), NETWORK_BATCH_SIZE).await;
        let peers = config.get_peers(config.local.server_id);
        let owd_states = peers
            .iter()
            .map(|&p| {
                (
                    p,
                    PeerOWDState {
                        window: VecDeque::with_capacity(OWD_WINDOW_SIZE),
                    },
                )
            })
            .collect();
        let deadline_send_array: HashMap<NodeId, i64> = peers.iter().map(|&p| (p, OWD_D_CAP)).collect();
        let receiver_d_cap: HashMap<NodeId, i64> = peers.iter().map(|&p| (p, OWD_D_CAP)).collect();
        OmniPaxosServer {
            id: config.local.server_id,
            database: Database::new(),
            network,
            omnipaxos,
            current_decided_idx: 0,
            omnipaxos_msg_buffer,
            clock,
            owd_states,
            deadline_send_array,
            receiver_d_cap,
            peers,
            config,

            early_buffer_sleep: Box::pin(sleep(Duration::from_secs(24 * 60 * 60 * 365))),
            early_buffer_timer_armed: false,
        }
    }

    pub async fn run(&mut self) {
        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        self.establish_initial_leader(&mut cluster_msg_buf, &mut client_msg_buf)
            .await;
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        let mut owd_probe_interval = tokio::time::interval(OWD_PROBE_INTERVAL);
        let mut owd_complete = false;
        loop {
            tokio::select! {
                _ = election_interval.tick() => {
                    self.omnipaxos.tick();
                    self.send_outgoing_msgs();
                },
                _ = owd_probe_interval.tick(), if !owd_complete => {
                    self.send_owd_probes();
                    if self.owd_probing_complete() {
                        info!("{}: OWD windows full — D={} µs, probing complete", self.id, OWD_D_CAP);
                        self.save_output().expect("Failed to write output");
                        owd_complete = true;
                    }
                },
                _ = self.network.cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
                },
                _ = self.network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(&mut client_msg_buf).await;
                },

                _ = &mut self.early_buffer_sleep, if self.early_buffer_timer_armed => {
                self.omnipaxos.process_early_buffer();
                self.reset_early_buffer_timer();
                self.send_outgoing_msgs();
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

    fn reset_early_buffer_timer(&mut self) {
        match self.omnipaxos.time_until_next_early_buffer_deadline() {
            Some(wait_us) => {
                let wait = Duration::from_micros(wait_us.max(0) as u64);
                let when = Instant::now() + wait;
                self.early_buffer_sleep.as_mut().reset(when); // Timer is ready when 'when' is reached.
                self.early_buffer_timer_armed = true;
            }
            None => {
                let disarm_when = Instant::now() + Duration::from_secs(24 * 60 * 60 * 365); // Safe 'when' at 1 year.
                self.early_buffer_sleep.as_mut().reset(disarm_when);
                self.early_buffer_timer_armed = false;
            }
        }
    }

    fn handle_decided_entries(&mut self) {
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_idx)
                .unwrap();
            self.current_decided_idx = new_decided_idx;
            debug!("Decided {new_decided_idx}");
            let decided_commands: Vec<Command> = decided_entries
                .into_iter()
                .filter_map(|e| match e {
                    LogEntry::Decided(cmd) => Some(cmd),
                    _ => unreachable!(),
                })
                .collect();
            self.update_database_and_respond(decided_commands);
        }
    }

    fn update_database_and_respond(&mut self, commands: Vec<Command>) {
        for command in commands {
            let read = self.database.handle_command(command.kv_cmd);
            if command.coordinator_id == self.id {
                let response = match read {
                    Some(read_result) => ServerMessage::Read(command.id, read_result),
                    None => ServerMessage::Write(command.id),
                };
                self.network.send_to_client(command.client_id, response);
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

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        let send_time = self.clock.get_time();
        let sigma_s = self.clock.get_uncertainty();
        // Deadline = now + max OWD estimate across all peers (µs)
        // Use estimates previously returned by receivers — not local windows.
        // Current request must not wait for new feedback (preserves 1 RTT fast path).
        let max_owd = self.peers.iter()
            .map(|p| *self.deadline_send_array.get(p).unwrap_or(&OWD_D_CAP))
            .max()
            .unwrap_or(OWD_D_CAP);
        let deadline = send_time + max_owd;

        for (from, message) in messages.drain(..) {
            // Multicast to all peers with deadline
            let peers: Vec<NodeId> = self.peers.clone();
            for peer in peers {
                self.network.send_to_cluster(peer, ClusterMessage::MulticastClientMessage {
                    client_id: from,
                    message: message.clone(),
                    deadline,
                    sent_time: send_time,
                    sender_uncertainty: sigma_s,
                    coordinator_id: self.id,
                });
            }
            // Append to local OmniPaxos log
            match message {
                ClientMessage::Append(command_id, kv_command) => {
                    self.append_to_log(from, command_id, kv_command, deadline, self.id);
                    self.reset_early_buffer_timer();
                }
            }
        }
        self.send_outgoing_msgs();
    }

    async fn handle_cluster_messages(
        &mut self,
        messages: &mut Vec<(NodeId, ClusterMessage)>,
    ) -> bool {
        let mut received_start_signal = false;
        for (from, message) in messages.drain(..) {
            trace!("{}: Received {message:?}", self.id);
            match message {
                ClusterMessage::OmniPaxosMessage(m) => {
                    self.omnipaxos.handle_incoming(m);
                    self.handle_decided_entries();
                    self.reset_early_buffer_timer();
                }
                ClusterMessage::LeaderStartSignal(start_time) => {
                    debug!("Received start message from peer {from}");
                    received_start_signal = true;
                    self.send_client_start_signals(start_time);
                }
                ClusterMessage::OWDProbe { send_time, sender_uncertainty } => {
                    // Bootstrap probe: receiver records sample, estimates OWD, sends feedback.
                    let raw_owd = self.clock.get_time() - send_time;
                    self.record_owd_sample(from, raw_owd);
                    self.update_receiver_d_cap(from);
                    let est = self.estimate_owd(from, sender_uncertainty);
                    self.network.send_to_cluster(from, ClusterMessage::OWDFeedback { estimated_owd: est });
                }
                ClusterMessage::OWDFeedback { estimated_owd } => {
                    // Sender stores the latest estimate returned by peer 'from'.
                    self.deadline_send_array.insert(from, estimated_owd);
                }
                ClusterMessage::MulticastClientMessage { client_id, message, deadline, sent_time, sender_uncertainty, coordinator_id } => {
                    // Receiver: update sliding window, adapt D_cap, compute estimate, send feedback.
                    // This is for SUBSEQUENT deadlines, current request proceeds immediately.
                    let raw_owd = self.clock.get_time() - sent_time;
                    self.record_owd_sample(from, raw_owd);
                    self.update_receiver_d_cap(from);
                    let est = self.estimate_owd(from, sender_uncertainty);
                    self.network.send_to_cluster(from, ClusterMessage::OWDFeedback { estimated_owd: est });
                    debug!("{}: Received multicast from {} deadline={} µs raw_owd={} µs", self.id, from, deadline, raw_owd);
                    match message {
                        ClientMessage::Append(command_id, kv_command) => {
                            self.append_to_log(client_id, command_id, kv_command, deadline, coordinator_id);
                        }
                    }
                }
            }
        }
        self.send_outgoing_msgs();
        received_start_signal
    }

    fn append_to_log(&mut self, from: ClientId, command_id: CommandId, kv_command: KVCommand, deadline : i64, coordinator_id: NodeId) {
        
        let command = Command {
            client_id: from,
            coordinator_id: coordinator_id,
            id: command_id,
            kv_cmd: kv_command,
            deadline: deadline,
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

    // -------------------------------------------------------------------------
    // OWD probing
    // -------------------------------------------------------------------------

    /// Sends one OWDProbe to every peer whose window is not yet full.
    fn send_owd_probes(&mut self) {
        let send_time = self.clock.get_time();
        let peers: Vec<NodeId> = self.peers.clone();
        for peer in peers {
            let window_full = self.owd_states
                .get(&peer)
                .map(|s| s.window.len() >= OWD_WINDOW_SIZE)
                .unwrap_or(false);
            if !window_full {
                self.network.send_to_cluster(peer, ClusterMessage::OWDProbe {
                    send_time,
                    sender_uncertainty: self.clock.get_uncertainty(),
                });
            }
        }
    }

    /// Records one raw OWD sample (µs) from `peer` into the sliding window.
    /// P (p95 of window) is recomputed on demand in estimate_owd().
    fn record_owd_sample(&mut self, peer: NodeId, raw_owd: i64) {
        let state = match self.owd_states.get_mut(&peer) {
            Some(s) => s,
            None => return,
        };
        if state.window.len() == OWD_WINDOW_SIZE {
            state.window.pop_front();
        }
        state.window.push_back(raw_owd);
        debug!(
            "{}: OWD sample from peer {} = {} µs ({} in window)",
            self.id, peer, raw_owd, state.window.len()
        );
    }

    /// Adaptively grows D_cap for `sender` if observed delays exceed the current cap.
    fn update_receiver_d_cap(&mut self, sender: NodeId) {
        let window_max = self.owd_states.get(&sender)
            .and_then(|s| s.window.iter().cloned().max())
            .unwrap_or(0);
        let adaptive = (window_max as f64 * 1.2) as i64;
        let entry = self.receiver_d_cap.entry(sender).or_insert(OWD_D_CAP);
        if adaptive > *entry {
            *entry = adaptive;
        }
    }

    /// Returns true once every peer's D_cap has been refined by at least one real sample.
    fn owd_probing_complete(&self) -> bool {
        self.peers.iter().all(|p| {
            self.owd_states.get(p).map(|s| s.window.len() >= OWD_WINDOW_SIZE).unwrap_or(false)
        })
    }

 
    #[allow(dead_code)]
    pub fn update_owd_window(&mut self, peer: NodeId, raw_owd: i64) {
        if let Some(state) = self.owd_states.get_mut(&peer) {
            if state.window.len() == OWD_WINDOW_SIZE {
                state.window.pop_front();
            }
            state.window.push_back(raw_owd);
        }
    }

    pub fn estimate_owd(&self, peer: NodeId, sender_uncertainty: i64) -> i64 {
        let d_cap = *self.receiver_d_cap.get(&peer).unwrap_or(&OWD_D_CAP);
        let state = match self.owd_states.get(&peer) {
            Some(s) if !s.window.is_empty() => s,
            _ => return d_cap,
        };
        // P = p95 of whatever samples are in the window (works for any window size ≥ 1).
        let mut sorted: Vec<i64> = state.window.iter().cloned().collect();
        sorted.sort_unstable();
        let idx = ((sorted.len() as f64 * 0.95).ceil() as usize).saturating_sub(1);
        let p = sorted[idx.min(sorted.len() - 1)];
        let sigma_r = self.clock.get_uncertainty();
        let est = p + (OWD_BETA * ((sender_uncertainty + sigma_r) as f64)) as i64;
        est.max(1).min(d_cap)
    }

    // ------------------Save the output ----------------------------------------------------

    fn save_output(&mut self) -> Result<(), std::io::Error> {
        #[derive(Serialize)]
        struct PeerOWDOutput {
            peer: NodeId,
            samples_us: Vec<i64>,
            p95_us: i64,
        }
        #[derive(Serialize)]
        struct Output<'a> {
            #[serde(flatten)]
            config: &'a OmniPaxosKVConfig,
            owd_d_cap: i64,
            owd_windows: Vec<PeerOWDOutput>,
        }

        let owd_windows: Vec<PeerOWDOutput> = {
            let mut entries: Vec<_> = self.owd_states.iter().collect();
            entries.sort_by_key(|(id, _)| *id);
            entries
                .into_iter()
                .map(|(peer, state)| {
                    let samples_us: Vec<i64> = state.window.iter().cloned().collect();
                    let p95_us = if samples_us.is_empty() {
                        OWD_D_CAP
                    } else {
                        let mut sorted = samples_us.clone();
                        sorted.sort_unstable();
                        let idx = ((sorted.len() as f64 * 0.95).ceil() as usize)
                            .saturating_sub(1)
                            .min(sorted.len() - 1);
                        sorted[idx]
                    };
                    PeerOWDOutput { peer: *peer, samples_us, p95_us }
                })
                .collect()
        };

        let output = Output {
            config: &self.config,
            owd_d_cap: OWD_D_CAP,
            owd_windows,
        };
        let config_json = serde_json::to_string_pretty(&output)?;
        let mut output_file = File::create(&self.config.local.output_filepath)?;
        output_file.write_all(config_json.as_bytes())?;
        output_file.flush()?;
        Ok(())
    }
}