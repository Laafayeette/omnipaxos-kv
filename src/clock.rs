
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


    pub fn get_time(&self) -> i64{

        let elapsed_real_secs = self.last_sync_system.elapsed().as_secs_f64();

        // Calculate the drift in microseconds: (seconds elapsed * drift rate)
        let drift_offset = (elapsed_real_secs * self.drift_rate) as i64;
        let elapsed_micros = (elapsed_real_secs * 1_000_000.0) as i64;

        // Simulated time = Last Synced Time + Real Elapsed (in micros) + Drift
        self.last_sync_simulated + elapsed_micros + drift_offset
    }

    pub fn get_uncertainty(&self) -> i64{
        self.uncertainty
    }



    pub fn sync_clock(&mut self){
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        self.last_sync_simulated = now;
        self.last_sync_system = Instant::now();

    }


}

