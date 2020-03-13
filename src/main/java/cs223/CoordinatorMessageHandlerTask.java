package cs223;

import java.util.*;

public class CoordinatorMessageHandlerTask implements Runnable {
    public static final int VOTE_IN_PROGRESS = 0;
    public static final int VOTE_ABORT = 1;
    public static final int VOTE_COMMIT = 2;
    private MultiNodeEmulator nm;
    public Map<String, Set<String>> preparing; // Tx ID to Set of involved cohorts
    public Map<String, Integer> voteCount; // Tx ID to count of votes received
    public Map<String, Integer> ackCount; // Tx ID to count of ACKs received
    public Map<String, Long> voting; // Tx ID to vote starting timestamp
    public Map<String, Integer> voteStatus; // Tx ID to decision (COMMIT, ABORT, COMPLETED)

    public CoordinatorMessageHandlerTask(MultiNodeEmulator nm) {
        this.nm = nm;
        this.preparing = new HashMap<>();
        this.voteCount = new HashMap<>();
        this.ackCount = new HashMap<>();
        this.voting = new HashMap<>();
        this.voteStatus = new HashMap<>();
    }

    public void run() {
        while (true) {
            EmulatedMessage em;
            try {
                em = nm.MessageQueues.get(0).take();
            }
            catch (InterruptedException e) {
                continue;
            }

            try {
                switch(em.op) {
                    /*
                    OP_PREPARE: Message received from client receiver.
                    sender: Tx ID
                    content: space separated cohort list
                    Action: put all cohorts into the preparing map.
                    */
                    case EmulatedMessage.OP_PREPARE: {
                        String[] cohortsArray = em.content.trim().split("\\s+");
                        Set<String> cohortsSet = new HashSet<>();
                        for (int i = 0; i < cohortsArray.length; i++) {
                            cohortsSet.add(cohortsArray[i]);
                        }
                        //System.out.println(cohortsSet);
                        preparing.put(em.sender, cohortsSet);
                        voteCount.put(em.sender, 0);
                        voting.put(em.sender, System.currentTimeMillis());
                        voteStatus.put(em.sender, VOTE_IN_PROGRESS);
                        break;
                    }

                    case EmulatedMessage.OP_VOTE_YES: {
                        String TxID = em.content;
                        if (!preparing.containsKey(TxID)) {
                            continue;
                        }
                        int count = voteCount.get(TxID) + 1;
                        voteCount.put(TxID, count);
                        if (preparing.get(TxID).size() == count) {
                            if (voteStatus.get(TxID) == VOTE_IN_PROGRESS) {
                                voteStatus.put(TxID, VOTE_COMMIT);
                            } else {
                                voteStatus.put(TxID, VOTE_ABORT);
                            }
                        }

                        if (voteStatus.get(TxID) == VOTE_COMMIT) {
                            // TODO: Write commit to log

                            // Send COMMIT to all cohorts
                            for (String cohortStr: preparing.get(TxID)) {
                                int cohortNum = Integer.parseInt(cohortStr);
                                nm.MessageQueues.get(cohortNum).put(new EmulatedMessage(
                                        "",
                                        EmulatedMessage.OP_COMMIT,
                                        TxID
                                ));
                            }
                            // Set ACK count to 0;
                            ackCount.put(TxID, 0);
                        } else if (voteStatus.get(TxID) == VOTE_ABORT) {
                            // Send ABORT to all cohorts
                            for (String cohortStr: preparing.get(TxID)) {
                                int cohortNum = Integer.parseInt(cohortStr);
                                nm.MessageQueues.get(cohortNum).put(new EmulatedMessage(
                                        "",
                                        EmulatedMessage.OP_ABORT,
                                        TxID
                                ));
                            }
                            // Set ACK count to 0;
                            ackCount.put(TxID, 0);
                        } else {
                            // Do nothing...
                        }
                        break;
                    }
                    case EmulatedMessage.OP_VOTE_NO: {
                        String TxID = em.content;
                        if (!preparing.containsKey(TxID)) {
                            continue;
                        }
                        int count = voteCount.get(TxID) + 1;
                        voteCount.put(TxID, count);
                        voteStatus.put(TxID, VOTE_ABORT);

                        // All votes are collected
                        if (preparing.get(TxID).size() == count) {
                            // Send ABORT to all cohorts
                            for (String cohortStr: preparing.get(TxID)) {
                                int cohortNum = Integer.parseInt(cohortStr);
                                nm.MessageQueues.get(cohortNum).put(new EmulatedMessage(
                                        "",
                                        EmulatedMessage.OP_ABORT,
                                        TxID
                                ));
                            }
                            // Set ACK count to 0;
                            ackCount.put(TxID, 0);
                        }

                        break;
                    }
                    case EmulatedMessage.OP_ACK: {
                        String TxID = em.content;
                        if (!preparing.containsKey(TxID)) {
                            continue;
                        }
                        int count = ackCount.get(TxID) + 1;
                        ackCount.put(TxID, count);
                        if (preparing.get(TxID).size() == count) {
                            // TODO: Write Completion Log

                            // TODO: Delete all info of Tx in memory
                            preparing.remove(TxID);
                            voteCount.remove(TxID);
                            ackCount.remove(TxID);
                            voteStatus.remove(TxID);
                        } else {
                            break;
                        }
                        break;
                    }
                    default: {
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
