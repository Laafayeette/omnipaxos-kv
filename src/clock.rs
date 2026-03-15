// The single source of truth for ClockSimulator lives in the omnipaxos library.
// Re-export so that `omnipaxos_kv::clock` consumers (tests, server code) keep working unchanged.
pub use omnipaxos::clock::{ClockSimError, ClockSimulator};
