package cs223;

import java.util.Hashtable;

public class CohortTimeoutCheckingTask implements Runnable {

    private MultiNodeEmulator nm;
    private int CohortNum;
    public Hashtable<String, Long> timeOfPrepare;

    // Testing purpose
    public CrashEmulation ce;

    public CohortTimeoutCheckingTask(MultiNodeEmulator nm, int CohortNum) {
        this.nm = nm;
        this.CohortNum = CohortNum;
        this.timeOfPrepare = nm.TransactionTimestamps.get(CohortNum);

        this.ce = nm.CrashEmulationConfigs.get(CohortNum);
    }

    public void run() {
        while (true) {
            if (ce.CRASH_NOW) {return;}
            for (String TxID: timeOfPrepare.keySet()) {
                if (ce.CRASH_NOW) {return;}
                if (timeOfPrepare.get(TxID) + Settings.P2_DECISION_TIMEOUT_MILISECOND < System.currentTimeMillis()) {
                    System.out.println("Cohort " + CohortNum + " Decision timeout, checking coordinator status...");
                    if (ce.CRASH_NOW) {return;}
                    try {
                        nm.MessageQueues.get(0).put(new EmulatedMessage(
                                ""+CohortNum,
                                EmulatedMessage.OP_COORD_STATUS,
                                TxID
                        ));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (ce.CRASH_NOW) {return;}
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {

            }
        }
    }
}
