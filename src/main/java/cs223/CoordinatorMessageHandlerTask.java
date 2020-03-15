package cs223;

import java.sql.*;
import java.util.*;

import static cs223.Settings.*;

public class CoordinatorMessageHandlerTask implements Runnable {
    private MultiNodeEmulator nm;

    public Map<String, Set<String>> preparing; // Tx ID to Set of involved cohorts
    public Map<String, Integer> voteCount; // Tx ID to count of votes received
    public Map<String, Integer> ackCount; // Tx ID to count of ACKs received
    //public Map<String, Long> voting; // Tx ID to vote starting timestamp
    public Map<String, Integer> voteStatus; // Tx ID to decision (COMMIT, ABORT, COMPLETED)

    private Hashtable<String, Long> timeOfDecision;
    private Hashtable<String, Long> timeOfVoteStart;

    // Testing purpose
    public CrashEmulation ce;

    public CoordinatorMessageHandlerTask(MultiNodeEmulator nm) {
        this.nm = nm;
        this.preparing = nm.preparing;
        this.voteCount = new HashMap<>();
        this.ackCount = nm.ackCount;
        //this.voting = new HashMap<>();
        this.voteStatus = nm.voteStatus;

        this.timeOfDecision = nm.TransactionTimestamps.get(0);
        this.timeOfVoteStart = nm.TransactionTimestamps.get(NUM_COHORTS + 1);

        this.ce = nm.CrashEmulationConfigs.get(0);
    }

    public void forceCommitLog(String TxID) {
        Connection con = null;
        Statement st = null;
        try {
            con = DriverManager.getConnection(Settings.COORD_LOG, Settings.P2_DB_USER, Settings.P2_DB_PASSWORD);
            con.setTransactionIsolation(Settings.ISOLATION_LEVEL);
            con.setAutoCommit(false);

            st = con.createStatement();
            boolean rs = st.execute("INSERT INTO commitedTx VALUES (\'" + TxID + "\');");
            st.close();

            con.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (!st.isClosed()) {
                    st.close();
                }
                con.close();
            } catch (SQLException e) {
                System.out.println("Failure to close connection.");
            }
        }
    }

    public void forceCompletionLog(String TxID) {
        Connection con = null;
        Statement st = null;
        try {
            con = DriverManager.getConnection(Settings.COORD_LOG, Settings.P2_DB_USER, Settings.P2_DB_PASSWORD);
            con.setTransactionIsolation(Settings.ISOLATION_LEVEL);
            con.setAutoCommit(false);

            st = con.createStatement();
            boolean rs = st.execute("INSERT INTO completedTx VALUES (\'" + TxID + "\');");
            st.close();

            con.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (!st.isClosed()) {
                    st.close();
                }
                con.close();
            } catch (SQLException e) {
                System.out.println("Failure to close connection.");
            }
        }
    }

    public boolean checkStatus(String TxID) {
        boolean found = false;
        Connection con = null;
        Statement st = null;
        try {


            con = DriverManager.getConnection(Settings.COORD_LOG, Settings.P2_DB_USER, Settings.P2_DB_PASSWORD);
            con.setTransactionIsolation(Settings.ISOLATION_LEVEL);
            con.setAutoCommit(false);

            st = con.createStatement();
            ResultSet rs = st.executeQuery("select TxID from commitedTx;");
            while (rs.next()) {
                String resultTxID = rs.getString(1);
                if (resultTxID.equals(TxID)) {
                    found = true;
                }
            }

            rs.close();

            st.close();

            con.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (!st.isClosed()) {
                    st.close();
                }
                con.close();
            } catch (SQLException e) {
                System.out.println("Failure to close connection.");
            }
        }

        return found;
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

            System.out.println("Coordinator message received from "+em.sender+ " : operation "+em.op+ ", content "+em.content);

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
                        //voting.put(em.sender, System.currentTimeMillis());
                        voteStatus.put(em.sender, VOTE_IN_PROGRESS);
                        // TODO: set timestamp for starting prepare
                        timeOfVoteStart.put(em.sender, System.currentTimeMillis());
                        break;
                    }

                    case EmulatedMessage.OP_VOTE_YES: {
                        String TxID = em.content;
                        System.out.println("Coordinator message received from cohort "+em.sender+" on TX "+TxID+": VOTE YES");

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
                        } else {
                            break;
                        }

                        if (voteStatus.get(TxID) == VOTE_COMMIT) {
                            // TODO: Write commit to log
                            forceCommitLog(TxID);
                            // Remove prepare timestamp
                            timeOfVoteStart.remove(TxID);
                            // Save current timestamp
                            timeOfDecision.put(TxID, System.currentTimeMillis());
                            // Send COMMIT to all cohorts
                            for (String cohortStr: preparing.get(TxID)) {
                                int cohortNum = Integer.parseInt(cohortStr);
                                nm.MessageQueues.get(cohortNum).put(new EmulatedMessage(
                                        "",
                                        EmulatedMessage.OP_COMMIT,
                                        TxID
                                ));

                                // Crash for testing purpose
                                if (ce.COORDINATOR_CRASH_AFTER_DECISION) {
                                    ce.CRASH_NOW = true;
                                    throw new Exception("Crashed for testing purpose after sending one commit.");
                                }
                            }
                            // Set ACK count to 0;
                            ackCount.put(TxID, 0);
                        } else if (voteStatus.get(TxID) == VOTE_ABORT) {
                            // Remove prepare timestamp
                            timeOfVoteStart.remove(TxID);
                            // Save current timestamp
                            timeOfDecision.put(TxID, System.currentTimeMillis());
                            // Send ABORT to all cohorts
                            for (String cohortStr: preparing.get(TxID)) {
                                int cohortNum = Integer.parseInt(cohortStr);
                                nm.MessageQueues.get(cohortNum).put(new EmulatedMessage(
                                        "",
                                        EmulatedMessage.OP_ABORT,
                                        TxID
                                ));

                                // Crash for testing purpose
                                if (ce.COORDINATOR_CRASH_AFTER_DECISION) {
                                    ce.CRASH_NOW = true;
                                    throw new Exception("Crashed for testing purpose after sending one abort.");
                                }
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
                        //System.out.println("Coordinator message received from cohort "+em.sender+" on TX "+TxID+": VOTE NO");
                        if (!preparing.containsKey(TxID)) {
                            continue;
                        }
                        int count = voteCount.get(TxID) + 1;
                        voteCount.put(TxID, count);
                        voteStatus.put(TxID, VOTE_ABORT);

                        // All votes are collected
                        if (preparing.get(TxID).size() == count) {
                            // Remove prepare timestamp
                            timeOfVoteStart.remove(TxID);
                            // Save current timestamp
                            timeOfDecision.put(TxID, System.currentTimeMillis());
                            // Send ABORT to all cohorts
                            for (String cohortStr: preparing.get(TxID)) {
                                int cohortNum = Integer.parseInt(cohortStr);
                                nm.MessageQueues.get(cohortNum).put(new EmulatedMessage(
                                        "",
                                        EmulatedMessage.OP_ABORT,
                                        TxID
                                ));

                                // Crash for testing purpose
                                if (ce.COORDINATOR_CRASH_AFTER_DECISION) {
                                    ce.CRASH_NOW = true;
                                    throw new Exception("Crashed for testing purpose after sending one abort.");
                                }
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
                        if (!preparing.get(TxID).contains(em.sender)) {
                            continue;
                        }
                        int count = ackCount.get(TxID) + 1;
                        ackCount.put(TxID, count);
                        preparing.get(TxID).remove(em.sender);

                        if (preparing.get(TxID).size() == 0) {
                            // TODO: Write Completion Log
                            forceCompletionLog(TxID);
                            // TODO: Delete all info of Tx in memory
                            preparing.remove(TxID);
                            voteCount.remove(TxID);
                            ackCount.remove(TxID);
                            voteStatus.remove(TxID);
                            timeOfDecision.remove(TxID);

                            nm.CONCURRENT_TRANSACTION_LOCK.release();
                        } else {
                            break;
                        }
                        break;
                    }
                    case EmulatedMessage.OP_COORD_STATUS:
                    {
                        // TODO:
                        // Query the commit log
                        // If contains the TxID then reply COMMIT. else reply ABORT
                        String TxID = em.content.split("C")[0];
                        boolean foundInCommited = checkStatus(TxID);
                        if (foundInCommited) {
                            nm.MessageQueues.get(Integer.parseInt(em.sender)).put(new EmulatedMessage(
                                    "",
                                    EmulatedMessage.OP_COMMIT,
                                    TxID
                            ));
                        } else {
                            nm.MessageQueues.get(Integer.parseInt(em.sender)).put(new EmulatedMessage(
                                    "",
                                    EmulatedMessage.OP_ABORT,
                                    TxID
                            ));
                        }
                        break;
                    }
                    default: {
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                if (ce.CRASH_NOW) {
                    return;
                }
            }
        }
    }
}
