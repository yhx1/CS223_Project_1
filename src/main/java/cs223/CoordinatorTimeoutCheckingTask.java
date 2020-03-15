package cs223;

import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import static cs223.Settings.*;

public class CoordinatorTimeoutCheckingTask implements Runnable {
    private MultiNodeEmulator nm;
    public Hashtable<String, Long> timeOfDecision;
    public Hashtable<String, Long> timeOfVoteStart;
    public Map<String, Set<String>> preparing; // Tx ID to Set of involved cohorts
    public Map<String, Integer> ackCount; // Tx ID to count of ACKs received
    public Map<String, Integer> voteStatus; // Tx ID to decision (COMMIT, ABORT, COMPLETED)

    // Testing purpose
    public CrashEmulation ce;

    public CoordinatorTimeoutCheckingTask(MultiNodeEmulator nm) {
        this.nm = nm;
        this.timeOfDecision = nm.TransactionTimestamps.get(0);
        this.timeOfVoteStart = nm.TransactionTimestamps.get(NUM_COHORTS+1);
        this.preparing = nm.preparing;
        this.ackCount = nm.ackCount;
        this.voteStatus = nm.voteStatus;

        this.ce = nm.CrashEmulationConfigs.get(0);
    }

    public void run() {

        while (true) {
            if (ce.CRASH_NOW) {return;}
            // Check Vote Timeouts
            for (String TxID: timeOfVoteStart.keySet()) {
                if (ce.CRASH_NOW) {return;}
                if (this.timeOfVoteStart.get(TxID) + P2_PREPARE_TIMEOUT_MILLISECOND < System.currentTimeMillis()) {
                    System.out.println("Vote timeout");
                    // Remove prepare timestamp
                    timeOfVoteStart.remove(TxID);
                    // Save current timestamp
                    timeOfDecision.put(TxID, System.currentTimeMillis());
                    // Send ABORT to all cohorts
                    try {
                        if (ce.CRASH_NOW) {return;}
                        for (String cohortStr: preparing.get(TxID)) {
                            if (ce.CRASH_NOW) {return;}
                            int cohortNum = Integer.parseInt(cohortStr);
                            nm.MessageQueues.get(cohortNum).put(new EmulatedMessage(
                                    "",
                                    EmulatedMessage.OP_ABORT,
                                    TxID
                            ));
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    // Set ACK count to 0;
                    ackCount.put(TxID, 0);
                    voteStatus.put(TxID, VOTE_ABORT);
                }
            }

            // Check ACK Timeouts
            for (String TxID: timeOfDecision.keySet()) {
                if (ce.CRASH_NOW) {return;}
                if (preparing.keySet().contains(TxID)) {
                    if (this.timeOfDecision.get(TxID) + Settings.P2_ACK_TIMEOUT_MILISECOND < System.currentTimeMillis()) {
                        System.out.println("ACK timeout.");
                        //ackCount.replace(TxID, 0);
                        String OP = "";
                        if (voteStatus.get(TxID) == VOTE_ABORT) {
                            OP = EmulatedMessage.OP_ABORT;
                        } else if (voteStatus.get(TxID) == VOTE_COMMIT) {
                            OP = EmulatedMessage.OP_COMMIT;
                        } else {
                            continue;
                        }

                        try {
                            for (String cohortNum: preparing.get(TxID)) {
                                if (ce.CRASH_NOW) {return;}
                                System.out.println("Coordinator sending "+OP+" to Cohort "+cohortNum);
                                nm.MessageQueues.get(Integer.parseInt(cohortNum)).put(new EmulatedMessage(
                                        "",
                                        OP,
                                        TxID
                                ));
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                    }
                }
                else continue;
            }

            if (ce.CRASH_NOW) {return;}
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }
        }

    }
}
