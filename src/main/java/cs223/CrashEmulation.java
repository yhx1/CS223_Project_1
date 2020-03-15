package cs223;

public class CrashEmulation {
    public boolean COHORT_CRASH_BEFORE_LOG_PREPARE;
    public boolean COHORT_CRASH_BEFORE_VOTE;
    public boolean COHORT_CRASH_AFTER_VOTE;
    public boolean COORDINATOR_CRASH_AFTER_DECISION;
    public volatile boolean CRASH_NOW;

    public CrashEmulation() {
        COHORT_CRASH_BEFORE_LOG_PREPARE = false;
        COHORT_CRASH_BEFORE_VOTE = false;
        COHORT_CRASH_AFTER_VOTE = false;
        COORDINATOR_CRASH_AFTER_DECISION = false;
        CRASH_NOW = false;
    }

    public void reset() {
        COHORT_CRASH_BEFORE_LOG_PREPARE = false;
        COHORT_CRASH_BEFORE_VOTE = false;
        COHORT_CRASH_AFTER_VOTE = false;
        COORDINATOR_CRASH_AFTER_DECISION = false;
        CRASH_NOW = false;
    }
}
