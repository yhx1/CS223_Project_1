package cs223;

import jdk.nashorn.internal.ir.Block;

import java.sql.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;

public class CohortTask implements Runnable {

    private volatile MultiNodeEmulator nm;
    private int CohortNum;
    public BlockingQueue<EmulatedMessage> queue;
    public Hashtable<String, Long> timeOfPrepare;

    private Map<String, List<String>> unprepared; // Tx ID to list of operations
    private Set<String> prepared; // Set of Tx IDs

    // Testing purpose
    public CrashEmulation ce;

    //private boolean BLOCK;

    public CohortTask(MultiNodeEmulator nm, int CohortNum, BlockingQueue<EmulatedMessage> queue) {
        this.nm = nm;
        this.CohortNum = CohortNum;
        this.queue = queue;
        this.timeOfPrepare = nm.TransactionTimestamps.get(CohortNum);

        this.ce = nm.CrashEmulationConfigs.get(CohortNum);

        unprepared = new HashMap<>();
        prepared = new HashSet<>();

        //BLOCK = false;
    }

    public void recovery() {
        Connection con = null;
        Statement st = null;
        try {
            con = DriverManager.getConnection(Settings.COHORT_LOG_PREFIX + CohortNum, Settings.P2_DB_USER, Settings.P2_DB_PASSWORD);
            con.setTransactionIsolation(Settings.ISOLATION_LEVEL);
            con.setAutoCommit(false);

            st = con.createStatement();
            ResultSet rs = st.executeQuery("select TxID from preparedTx where TxID NOT IN (SELECT TxID from completedTx);");
            while (rs.next()) {
                String TxID = rs.getString(1);
                prepared.add(TxID);
                timeOfPrepare.put(TxID, 0L);
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
    }


    // Open connection to db, open prepared transaction
    // Execute all operations in the unprepared map entry
    // Prepare transaction
    // Force write prepare log
    public boolean prepare(String TxID) throws Exception{
        int success = 1;
        Connection con = null;
        Statement st = null;

        // Prepare transaction
        try {
            con = DriverManager.getConnection(Settings.COHORT_DB_PREFIX + CohortNum, Settings.P2_DB_USER, Settings.P2_DB_PASSWORD);
            con.setTransactionIsolation(Settings.ISOLATION_LEVEL);
            con.setAutoCommit(false);

            unprepared.get(TxID).add("PREPARE TRANSACTION \'" + TxID + "\';");
            for (int i = 0; i < unprepared.get(TxID).size(); i++) {
                st = con.createStatement();
                boolean rs = st.execute(unprepared.get(TxID).get(i));
                st.close();
            }

            con.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            success = 0;
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

        if (success == 0) {
            return false;
        }

        // Set prepare timestamp
        timeOfPrepare.put(TxID, System.currentTimeMillis());

        // Crash if configured
        if (ce.COHORT_CRASH_BEFORE_LOG_PREPARE) {
            ce.CRASH_NOW = true;
            throw new Exception("Crashed for testing purpose before prepare log is forced.");
        }

        // Force write prepare log
        try {
            con = DriverManager.getConnection(Settings.COHORT_LOG_PREFIX + CohortNum, Settings.P2_DB_USER, Settings.P2_DB_PASSWORD);
            con.setTransactionIsolation(Settings.ISOLATION_LEVEL);
            con.setAutoCommit(false);

            st = con.createStatement();
            boolean rs = st.execute("INSERT INTO preparedTx VALUES (\'" + TxID + "\');");
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

        prepared.add(TxID);
        unprepared.remove(TxID);
        return true;
    }

    // Commit prepared transaction
    // Force write completion log
    public void commit(String TxID) {
        Connection con = null;
        Statement st = null;

        // Commit prepared transaction
        try {
            con = DriverManager.getConnection(Settings.COHORT_DB_PREFIX + CohortNum, Settings.P2_DB_USER, Settings.P2_DB_PASSWORD);
            con.setTransactionIsolation(Settings.ISOLATION_LEVEL);
            con.setAutoCommit(true);

            st = con.createStatement();
            boolean rs = st.execute("COMMIT PREPARED \'" + TxID + "\';");
            st.close();
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

        // Force write completion log
        try {
            con = DriverManager.getConnection(Settings.COHORT_LOG_PREFIX + CohortNum, Settings.P2_DB_USER, Settings.P2_DB_PASSWORD);
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

    public void abort(String TxID) {
        Connection con = null;
        Statement st = null;

        // Commit prepared transaction
        try {
            con = DriverManager.getConnection(Settings.COHORT_DB_PREFIX + CohortNum, Settings.P2_DB_USER, Settings.P2_DB_PASSWORD);
            con.setTransactionIsolation(Settings.ISOLATION_LEVEL);
            con.setAutoCommit(true);

            st = con.createStatement();
            boolean rs = st.execute("ROLLBACK PREPARED \'" + TxID + "\';");
            st.close();
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

        // Force write completion log
        try {
            con = DriverManager.getConnection(Settings.COHORT_LOG_PREFIX + CohortNum, Settings.P2_DB_USER, Settings.P2_DB_PASSWORD);
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

    @Override
    public void run() {

        // Check logs to find out which Txs are prepared but not completed.
        // put them into the prepared set.

        System.out.println("Cohort "+ CohortNum + " started.");

        recovery();

        while (true) {
            EmulatedMessage em;
            try {
                em = queue.take();
            }
            catch (InterruptedException e) {
                continue;
            }

            System.out.println("Cohort "+CohortNum+ " message received from "+em.sender+ " : operation "+em.op+ ", content "+em.content);
            try {
                switch(em.op) {
                    case EmulatedMessage.OP_ECHO:
                    {
                        nm.MessageQueues.get(0).put(new EmulatedMessage(Integer.toString(CohortNum),
                                EmulatedMessage.OP_ECHO,
                                em.content));
                        break;
                    }
                    case EmulatedMessage.OP_SEND_OPERATION:
                        // Put operation in unprepared map
                    {
                        String TxID = em.sender + "C" + CohortNum;
                        if (!unprepared.containsKey(TxID)) {
                            unprepared.put(TxID, new ArrayList<>());
                            unprepared.get(TxID).add("BEGIN;");
                        }
                        unprepared.get(TxID).add(em.content);
                        break;
                    }
                    case EmulatedMessage.OP_PREPARE:
                        // Open connection to db, open prepared transaction
                        // Execute all operations in the unprepared map entry
                        // Prepare transaction
                        // Force write prepare log
                        // Answer to coordinator
                    {
                        String TxID = em.sender + "C" + CohortNum;
                        boolean success = prepare(TxID);

                        // Crash for testing
                        if (ce.COHORT_CRASH_BEFORE_VOTE) {
                            ce.CRASH_NOW = true;
                            throw new Exception("Crashed for testing purpose before voting.");
                        }

                        if (success) {
                            boolean putSuccess = false;
                            while (!putSuccess) {
                                try {
                                    nm.MessageQueues.get(0).put(new EmulatedMessage(Integer.toString(CohortNum),
                                            EmulatedMessage.OP_VOTE_YES,
                                            em.sender // Reply global transaction ID rather than the local one
                                    ));
                                    putSuccess = true;
                                } catch (InterruptedException e) {
                                    putSuccess = false;
                                }
                            }
                        } else {
                            boolean putSuccess = false;
                            while (!putSuccess) {
                                try {
                                    nm.MessageQueues.get(0).put(new EmulatedMessage(Integer.toString(CohortNum),
                                            EmulatedMessage.OP_VOTE_NO,
                                            em.sender // Reply global transaction ID rather than the local one
                                    ));
                                    putSuccess = true;
                                } catch (InterruptedException e) {
                                    putSuccess = false;
                                }
                            }
                        }

                        // Crash for testing
                        if (ce.COHORT_CRASH_AFTER_VOTE) {
                            ce.CRASH_NOW = true;
                            throw new Exception("Crashed for testing purpose after voting.");
                        }

                        break;
                    }
                    case EmulatedMessage.OP_COMMIT:
                        // Open connection to db
                        // Commit prepared transaction
                        // Force write completion log
                        // Ack
                    {
                        String TxID = em.content + "C" + CohortNum;
                        if (!prepared.contains(TxID)) {
                            break;
                        }
                        commit(TxID);

                        boolean success = false;
                        while (!success) {
                            try {
                                nm.MessageQueues.get(0).put(new EmulatedMessage(Integer.toString(CohortNum),
                                        EmulatedMessage.OP_ACK,
                                        em.content)); // Reply global transaction ID rather than the local one
                                success = true;
                            } catch (InterruptedException e) {
                                success = false;
                            }
                        }
                        prepared.remove(TxID);
                        timeOfPrepare.remove(TxID);
                        break;
                    }
                    case EmulatedMessage.OP_ABORT:
                        // Open connection to db
                        // Rollback prepared transaction
                        // Force write completion log
                        // Ack
                    {
                        String TxID = em.content + "C" + CohortNum;
                        /*
                        if (!prepared.contains(TxID)) {
                            break;
                        }

                         */
                        abort(TxID);
                        nm.MessageQueues.get(0).put(new EmulatedMessage(Integer.toString(CohortNum),
                                EmulatedMessage.OP_ACK,
                                em.content)); // Reply global transaction ID rather than the local one
                        prepared.remove(TxID);
                        timeOfPrepare.remove(TxID);
                        break;
                    }
                    default:
                    {
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
