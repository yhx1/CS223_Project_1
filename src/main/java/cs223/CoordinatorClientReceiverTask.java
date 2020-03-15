package cs223;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CoordinatorClientReceiverTask implements Runnable {

    private MultiNodeEmulator nm;

    public CoordinatorClientReceiverTask(MultiNodeEmulator nm) {
        this.nm = nm;
    }


    private void sendOperations(PreparedTransaction pt) {
        try {
            for (int i: pt.operations.keySet()) {
                for (int j = 0; j < pt.operations.get(i).size(); j++) {
                    String operation = pt.operations.get(i).get(j);
                    nm.MessageQueues.get(i).put(new EmulatedMessage(
                            pt.id,
                            EmulatedMessage.OP_SEND_OPERATION,
                            operation
                    ));
                }
                // SEND PREPARE TO ALL COHORTS
                nm.MessageQueues.get(i).put(new EmulatedMessage(
                        pt.id,
                        EmulatedMessage.OP_PREPARE,
                        ""
                ));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        // TODO: load decided Map from disk on task startup

        while (true) {
            PreparedTransaction pt;
            try {
                pt = nm.TransactionQueue.take();
            }
            catch (InterruptedException e) {
                continue;
            }

            //System.out.println("Coordinator messaged received from "+em.sender+ " : operation "+em.op+ ", content "+em.content);

            // Tell Coordinator Message Handler to wait for votes
            String participatingCohorts = "";
            for (int i: pt.operations.keySet()) {
                participatingCohorts = participatingCohorts + Integer.toString(i) + " ";
            }

            boolean acquired = false;
            while (!acquired) {
                try {
                    nm.CONCURRENT_TRANSACTION_LOCK.acquire();
                    acquired = true;
                } catch (InterruptedException e) {
                    acquired = false;
                }
            }
            try {
                nm.MessageQueues.get(0).put(new EmulatedMessage(
                        pt.id,
                        EmulatedMessage.OP_PREPARE,
                        participatingCohorts
                ));
            } catch (InterruptedException e ) {
                e.printStackTrace();
            }


            // Send messages to cohorts & Request to Prepare
            sendOperations(pt);

            // Wait for Votes

            // Write COMMIT/ABORT to log

            // Send COMMIT/ABORT

            // Wait for ACKs

            // Write Completion Log

            //
            //break;
        }
    }
}
