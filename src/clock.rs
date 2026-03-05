
use std::time::{Duration,Instant, SystemTime, UNIX_EPOCH};
pub struct ClockSimulator {
    drift_rate: f64,            // microseconds per second
    uncertainty: i64,           // microseconds
    last_sync_system: Instant,// system time
    sync_interval: Duration, // the sync interval
    last_sync_simulated: i64,   // micro seconds

}

impl ClockSimulator {

    pub fn new(drift_rate: f64,uncertainty: i64,sync_interval: Duration)-> Self{
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        Self{
            drift_rate,
            uncertainty,
            sync_interval,
            last_sync_system: Instant::now(),
            last_sync_simulated: now,
        }

    }


}

