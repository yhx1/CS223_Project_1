package cs223;

import java.util.*;
import java.util.concurrent.*;

public class MultiNodeEmulator {

    public List<BlockingQueue<EmulatedMessage>> MessageQueues = new ArrayList<>();
    public List<Hashtable<String, Long>> TransactionTimestamps = new ArrayList<>();
    public BlockingQueue<PreparedTransaction> TransactionQueue = new LinkedBlockingQueue<>();
    public List<CrashEmulation> CrashEmulationConfigs = new ArrayList<>();

    public Map<String, Set<String>> preparing; // Tx ID to Set of involved cohorts
    public Map<String, Integer> ackCount; // Tx ID to count of ACKs received
    public Map<String, Integer> voteStatus; // Tx ID to decision (COMMIT, ABORT, COMPLETED)

    public Semaphore CONCURRENT_TRANSACTION_LOCK = new Semaphore(1);

    public MultiNodeEmulator() {
        for (int i=0; i < Settings.NUM_COHORTS + 1; i++) {
            MessageQueues.add(new LinkedBlockingQueue<EmulatedMessage>());
            TransactionTimestamps.add(new Hashtable<String, Long>());
            CrashEmulationConfigs.add(new CrashEmulation());
        }
        TransactionTimestamps.add(new Hashtable<String, Long>());

        preparing = new HashMap<>();
        ackCount = new HashMap<>();
        voteStatus = new HashMap<>();
    }

}
