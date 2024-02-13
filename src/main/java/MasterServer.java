import java.io.PrintWriter;
import java.util.ArrayList;

public class MasterServer extends RedisServer{
    String role = "master";
    private static final String MASTER_REPLID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static final String MASTER_REPL_OFFSET = "0";
    public MasterServer(){
        super();
    }

    @Override
    public void handleCommand(ArrayList<String> commandArray, PrintWriter writer){
        int commandLength = Integer.parseInt(commandArray.getFirst().substring(1));
        String command = commandArray.get(2);
        switch (command.toLowerCase()){
            case "ping":
                handlePing(writer);
                break;
            case "echo":
                handleEcho(commandArray.get(4), writer);
                break;
            case "replconf":
                handleReplconf(writer);
                break;
            case "set":
                if(commandLength==3) {
                    handleSet(commandArray.get(4), commandArray.get(6), writer);
                }else if(commandLength==5){
                    if(commandArray.get(8).equalsIgnoreCase("px")){
                        handleSetWithExpiry(commandArray.get(4), commandArray.get(6), commandArray.get(10), writer);
                    }
                }else{
                    writer.print("-ERR syntax error\r\n");
                    writer.flush();
                }
                break;
            case "get":
                handleGet(commandArray.get(4), writer);
                break;
            case "info":
                if(commandArray.get(4).equalsIgnoreCase("replication")){
                    handleInfo(writer);
                }
                else{
                    writer.print("-ERR unknown subcommand or wrong number of arguments for 'info'\r\n");
                    writer.flush();
                }
                break;
            default:
                writer.print("-ERR unknown command '"+command+"'\r\n");
                writer.flush();
                break;
        }
    }

    public void handleReplconf(PrintWriter writer){
        writer.print("+OK\r\n");
        writer.flush();
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
