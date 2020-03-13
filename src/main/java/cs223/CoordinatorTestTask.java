package cs223;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CoordinatorTestTask implements Runnable {

    private MultiNodeEmulator nm;

    public CoordinatorTestTask(MultiNodeEmulator nm) {
        this.nm = nm;
    }

    @Override
    public void run() {
        while (true) {
            String line = "default";
            BufferedReader reader =
                    new BufferedReader(new InputStreamReader(System.in));

            // Reading data using readLine
            try {
                line = reader.readLine();

                for (int i = 1; i <= Settings.NUM_COHORTS; i++) {
                    nm.MessageQueues.get(i).put(new EmulatedMessage(
                            "Coord",
                            EmulatedMessage.OP_ECHO,
                            line
                    ));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }


        }
    }
}
