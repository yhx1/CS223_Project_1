package cs223;

import java.util.concurrent.Semaphore;

public class Metric {

    public Semaphore semaphore;
    public int NUM_TRANSACTIONS;
    public long TRANSACTION_TIME_ELAPSED;
    public int NUM_OPERATIONS;
    public long OPERATION_TIME_ELAPSED;
    public int NUM_QUERIES;
    public long QUERY_TIME_ELAPSED;

    public Metric() {
        semaphore = new Semaphore(1);
        NUM_TRANSACTIONS = 0;
        TRANSACTION_TIME_ELAPSED = 0L;
        NUM_OPERATIONS = 0;
        OPERATION_TIME_ELAPSED = 0L;
        NUM_QUERIES = 0;
        QUERY_TIME_ELAPSED = 0L;
    }

    public void printMetrics(int second) {
        System.out.println("Time Elapsed: " + second + "s");
        if (NUM_TRANSACTIONS != 0) {
            System.out.println("#Transactions: " + NUM_TRANSACTIONS + " #Average Delay: " + (TRANSACTION_TIME_ELAPSED/NUM_TRANSACTIONS) + "ms Throughput: " + ((float)NUM_TRANSACTIONS/second) + " trx/s");
        }
        if (NUM_OPERATIONS != 0) {
            System.out.println("#Operations: " + NUM_OPERATIONS + " #Average Operation Response Time: " + (OPERATION_TIME_ELAPSED * 1000/NUM_OPERATIONS) + "ns");
        }
        if (NUM_QUERIES != 0) {
            System.out.println("#Queries: " + NUM_QUERIES + " #Average Operation Response Time: " + (QUERY_TIME_ELAPSED * 1000/NUM_QUERIES) + "ns");
        }
    }
}
