import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class MasterServer extends RedisServer{
    String role = "master";
    private static final String MASTER_REPLID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static final String MASTER_REPL_OFFSET = "0";
    private final HashSet<Socket> replicaSockets;
    public MasterServer(){
        super();
        replicaSockets = new HashSet<>();
    }

    @Override
    public void handleClient(Socket clientSocket) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//            PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
            String line;
            ArrayList<String> commandArray = new ArrayList<>();
            int commandLength=0;
            while ((line = reader.readLine()) != null) {
                commandArray.add(line);
                if(commandArray.size()==1){
                    commandLength = Integer.parseInt(commandArray.getFirst().substring(1))*2+1;
                }
                else if(commandArray.size()==commandLength){
                    handleCommand(commandArray, clientSocket);
                    commandArray.clear();
                }
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }finally {
            try {
                if (clientSocket != null && !replicaSockets.contains(clientSocket)) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }

    public void handleCommand(ArrayList<String> commandArray, Socket clientSocket) throws IOException{
        int commandLength = Integer.parseInt(commandArray.getFirst().substring(1));
        OutputStream os = clientSocket.getOutputStream();
        PrintWriter writer = new PrintWriter(os, true);
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
            case "psync":
                handlePsync(writer);
                sendRDBFile(os);
                replicaSockets.add(clientSocket);
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
                propogateToReplicas(commandArray);
                break;
            case "get":
                handleGet(commandArray.get(4), writer);
                propogateToReplicas(commandArray);
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

    private void handleReplconf(PrintWriter writer){
        writer.print("+OK\r\n");
        writer.flush();
    }
    private void handlePsync(PrintWriter writer){
        writer.print(String.format("+FULLRESYNC %s %s\r\n", MASTER_REPLID, MASTER_REPL_OFFSET));
        writer.flush();
    }
    private void sendRDBFile(OutputStream os){
        BufferedOutputStream bos = new BufferedOutputStream(os);
        byte[] rdb= Base64.getDecoder().decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJp" +
                "dHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==");
        String prefix= "$" + rdb.length + "\r\n";
        byte[] prefixBytes=prefix.getBytes();
        try {
            bos.write(prefixBytes);
            bos.write(rdb);
            bos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void propogateToReplicas(ArrayList<String> commandArray){
        for(Socket replicaSocket: replicaSockets){
            try {
                PrintWriter writer = new PrintWriter(replicaSocket.getOutputStream(), true);
                for(String command: commandArray){
                    writer.print(command+"\r\n");
                }
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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
