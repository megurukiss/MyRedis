import java.io.PrintWriter;

public class MasterServer extends RedisServer{
    String role = "master";
    private static final String MASTER_REPLID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static final String MASTER_REPL_OFFSET = "0";
    public MasterServer(){
        super();
    }

    @Override
    public void handleInfo(PrintWriter writer){
        StringBuilder response = new StringBuilder();
        response.append("# Replication\r\n");
        response.append(String.format("role:%s\r\n", role));
        response.append(String.format("master_replid:%s\r\n", MASTER_REPLID));
        response.append(String.format("master_repl_offset:%s\r\n", MASTER_REPL_OFFSET));
        String info=bulkString(response.toString());
        writer.print(info);
        writer.flush();
    }
}
