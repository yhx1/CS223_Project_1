package cs223;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PreparedTransaction {
    public String id;
    public Map<Integer, List<String>> operations;

    public PreparedTransaction(String id) {
        this.id = id;
        this.operations = new HashMap<>();
    }
}
