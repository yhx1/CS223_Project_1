package cs223;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import static cs223.MySQLDataLoader.*;

public class MySQLTransactionTask implements Runnable {

    private ArrayList<String> statements;
    private Metric metric;
    private boolean readonly;
    private long taskStartTimeMillis;

    public MySQLTransactionTask(ArrayList<String> statements, Metric metric, boolean readonly, long taskStartTimeMillis) {
        this.statements = statements;
        this.metric = metric;
        this.readonly = readonly;
        this.taskStartTimeMillis = taskStartTimeMillis;
    }

    @Override
    public void run() {

        long transactionTimeMillis = 0;
        long operationDelayMillis = 0;

        try {
            Connection con = DriverManager.getConnection(DB_URL, DB_USER,DB_PASSWORD);
            con.setTransactionIsolation(Settings.ISOLATION_LEVEL);

            // For Queries
            if (readonly) {
                con.setReadOnly(readonly);
                for (int i = 0; i < statements.size(); i++) {
                    long operationStartTime = System.currentTimeMillis();
                    Statement st = con.createStatement();
                    ResultSet rs = st.executeQuery(statements.get(i));
                    rs.close();
                    st.close();
                    long operationEndTime = System.currentTimeMillis();
                    operationDelayMillis += (operationEndTime - operationStartTime);
                }
                con.close();
                long transactionEndTime = System.currentTimeMillis();
                transactionTimeMillis = transactionEndTime-taskStartTimeMillis;
            }
            // For Inserts
            else {
                con.setAutoCommit(false);
                for (int i=0; i < statements.size(); i++) {
                    long operationStartTime = System.currentTimeMillis();
                    Statement st = con.createStatement();
                    int rs = st.executeUpdate(statements.get(i));
                    st.close();
                    long operationEndTime = System.currentTimeMillis();
                    operationDelayMillis += (operationEndTime - operationStartTime);
                }

                con.commit();
                con.close();
                long transactionEndTime = System.currentTimeMillis();
                transactionTimeMillis = transactionEndTime - taskStartTimeMillis;
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

        // Update all metrics in critical section
        try {
            metric.semaphore.acquire();
            metric.NUM_TRANSACTIONS ++;
            metric.TRANSACTION_TIME_ELAPSED += transactionTimeMillis;
            metric.NUM_OPERATIONS += statements.size();
            metric.OPERATION_TIME_ELAPSED += operationDelayMillis;

            if (readonly) {
                metric.NUM_QUERIES = statements.size();
                metric.QUERY_TIME_ELAPSED += operationDelayMillis;
            }
            metric.semaphore.release();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
