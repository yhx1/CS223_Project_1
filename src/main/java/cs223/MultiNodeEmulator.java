package cs223;

import java.util.*;
import java.util.concurrent.*;

public class MultiNodeEmulator {

    public List<BlockingQueue<EmulatedMessage>> MessageQueues = new ArrayList<>();
    public BlockingQueue<PreparedTransaction> TransactionQueue = new LinkedBlockingQueue<>();

    public MultiNodeEmulator() {
        for (int i=0; i < Settings.NUM_COHORTS + 1; i++) {
            MessageQueues.add(new LinkedBlockingQueue<EmulatedMessage>());
        }

    }

    public static void main(final String[] args) throws Exception {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Settings.NUM_COHORTS+2);

        MultiNodeEmulator nm = new MultiNodeEmulator();
        CohortTask ct1 = new CohortTask(nm, 1, nm.MessageQueues.get(1));
        executor.execute(ct1);

        CoordinatorTestTask cmrt = new CoordinatorTestTask(nm);
        executor.execute(cmrt);

        CoordinatorClientReceiverTask ct = new CoordinatorClientReceiverTask(nm);
        executor.execute(ct);

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }


}
