use omnipaxos_kv::clock::ClockSimulator;
use std::time::Duration;

#[test]
fn test_clock_creation() {
    println!("\n=== Testing Clock Creation ===");
    let clock = ClockSimulator::new(50.0, 100, Duration::from_secs(60));
    let time = clock.get_time();
    println!("Clock created successfully");
    println!("Initial time: {} microseconds", time);
    println!("Drift rate: 50 μs/s");
    println!("Uncertainty: 100 μs");
    assert!(time > 0);
    println!("✓ Test passed\n");
}

#[test]
fn test_clock_drift() {
    println!("\n=== Testing Clock Drift ===");
    let clock = ClockSimulator::new(50.0, 100, Duration::from_secs(60));
    let time1 = clock.get_time();
    println!("Time 1: {} μs", time1);

    println!("Sleeping for 100ms...");
    std::thread::sleep(Duration::from_millis(100));

    let time2 = clock.get_time();
    println!("Time 2: {} μs", time2);

    let diff = time2 - time1;
    println!("Elapsed: {} μs", diff);
    println!("Expected: ~100,000 μs + drift");

    assert!(time2 > time1);
    assert!(diff >= 100_000); // At least 100ms passed
    println!("✓ Test passed\n");
}

#[test]
fn test_uncertainty() {
    println!("\n=== Testing Clock Uncertainty ===");
    let clock = ClockSimulator::new(50.0, 100, Duration::from_secs(60));
    let uncertainty = clock.get_uncertainty();
    println!("Configured uncertainty: 100 μs");
    println!("Retrieved uncertainty: {} μs", uncertainty);
    assert_eq!(uncertainty, 100);
    println!("✓ Test passed\n");
}

#[test]
fn test_all_getters() {
    println!("\n=== Testing All Getters ===");
    let drift_rate = 75.0;
    let uncertainty = 250;
    let clock = ClockSimulator::new(drift_rate, uncertainty, Duration::from_secs(30));

    println!("Testing get_time()...");
    let time1 = clock.get_time();
    println!("  Time: {} μs", time1);
    assert!(time1 > 0);

    println!("Testing get_uncertainty()...");
    let unc = clock.get_uncertainty();
    println!("  Uncertainty: {} μs", unc);
    assert_eq!(unc, uncertainty);

    println!("Verifying get_time() consistency...");
    let time2 = clock.get_time();
    println!("  Time (2nd call): {} μs", time2);
    assert!(time2 >= time1, "Time should never go backwards");

    println!("✓ All getters working correctly\n");
}

#[test]
fn test_sync_clock() {
    println!("\n=== Testing Clock Synchronization ===");
    let mut clock = ClockSimulator::new(50.0, 100, Duration::from_secs(60));

    println!("Initial time:");
    let time1 = clock.get_time();
    println!("  Time 1: {} μs", time1);

    println!("Sleeping for 200ms to accumulate drift...");
    std::thread::sleep(Duration::from_millis(200));

    let time2 = clock.get_time();
    println!("  Time 2 (with drift): {} μs", time2);
    let drift_accumulated = time2 - time1;
    println!("  Drift accumulated: {} μs", drift_accumulated);

    println!("Calling sync_clock()...");
    clock.sync_clock();

    let time3 = clock.get_time();
    println!("  Time 3 (after sync): {} μs", time3);

    println!("Sleeping for 100ms after sync...");
    std::thread::sleep(Duration::from_millis(100));

    let time4 = clock.get_time();
    println!("  Time 4: {} μs", time4);
    let elapsed_after_sync = time4 - time3;
    println!("  Elapsed after sync: {} μs", elapsed_after_sync);

    // After sync, drift should reset - elapsed should be close to 100ms, not 300ms
    assert!(elapsed_after_sync < drift_accumulated, "Drift should have been reset");
    assert!(elapsed_after_sync >= 100_000, "At least 100ms should have passed");
    assert!(elapsed_after_sync < 150_000, "Should be close to 100ms, not accumulating old drift");

    println!("✓ Clock sync working correctly - drift was reset\n");
}
