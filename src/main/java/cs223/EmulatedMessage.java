package cs223;

public class EmulatedMessage {

    public final static String OP_ECHO = "ECHO";
    public final static String OP_SEND_OPERATION = "SEND_OPERATION";
    public final static String OP_PREPARE = "PREPARE";
    public final static String OP_VOTE_YES = "VOTE_YES";
    public final static String OP_VOTE_NO = "VOTE_NO";
    public final static String OP_COMMIT = "COMMIT";
    public final static String OP_ABORT = "ABORT";
    public final static String OP_ACK = "ACK";
    public final static String OP_COORD_STATUS = "COORD_STATUS";

    public String sender;
    public String op;
    public String content;

    public EmulatedMessage(String sender, String op, String content) {
        this.sender = sender;
        this.op = op;
        this.content = content;
    }
}
