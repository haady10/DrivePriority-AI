"""Phase 5: day-by-day replay of simulated inspections under a fixed daily capacity.

Decision-support simulation only. Nobody inspected these drives and a simulated inspection changes
nothing on a drive, so no result here shows that failures are prevented. All rules and numbers are
fixed in docs/phase5_protocol.md and configs/phase5_protocol_v1.json before any replay result.

Modules (each one small and testable on its own):
    config         read the frozen JSON protocol
    hashing        machine-independent drive keys and tie keys
    replay_input   build / load the one-row-per-drive-day table the replay runs on
    ranking        daily priority order (score, then frozen tie key)
    transforms     RQ2 persistence rules as running statistics of the score
    engine         the daily state machine (fresh Top-K, carry-forward queue, cooldown)
    events         failure events, eligibility, timely capture, workload
    stability      watchlist turnover (RQ2)
    stats          contrasts, interactions and the drive-cluster bootstrap
    prediction     RQ3 prediction quality per horizon
    run_phase5     command line entry point
"""
