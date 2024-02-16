import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MasterServer extends RedisServer{
    String role = "master";
    private static final String MASTER_REPLID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static final String MASTER_REPL_OFFSET = "0";
    private final LinkedList<Socket> replicaSockets;
    public MasterServer(){
        super();
        replicaSockets = new LinkedList<>();
    }

    @Override
    public void startServer() {
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            new Thread(() -> {
                while (true) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        new Thread(() -> {
                            handleClient(clientSocket);
                        }).start();
                    } catch (IOException e) {
                        System.out.println("IOException: " + e.getMessage());
                    }
                }
            }).start();
            // start a thread for replicaSockets
            /*for (Socket replicaSocket : replicaSockets) {
                new Thread(() -> {
                    try {
                        listenToSocketCommand(replicaSocket);
                    } catch (IOException e) {
                        System.out.println("IOException: " + e.getMessage());
                        e.printStackTrace();
                    }
                }).start();
            }*/

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void handleClient(Socket clientSocket) {
        try {
            listenToSocketCommand(clientSocket);
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

    @Override
    public void handleCommand(ArrayList<String> commandArray, Socket clientSocket) throws IOException{
        int commandLength = Integer.parseInt(commandArray.getFirst().substring(1));
        OutputStream os = clientSocket.getOutputStream();
        PrintWriter writer = new PrintWriter(os, true);
        String command = commandArray.get(2);
        switch (command.toLowerCase()){
            case "ping":
                handlePing(writer);
                // check if the client is a replica
                /*addReplica(clientSocket);*/
                /*replicaSockets.add(clientSocket);*/
                // if client not in replicaSockets, add it
                if(!replicaSockets.contains(clientSocket)){
                    replicaSockets.add(clientSocket);
                    /*System.out.println(replicaSockets);*/
                    startRepliThread(clientSocket);
                }
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
                break;
            case "set":
                propogateToReplicas(commandArray);
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
            case "wait":
                handleWait(clientSocket);
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
        byte[] rdb= Base64.getDecoder().decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==");
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
    private void propogateToReplicas(ArrayList<String> commandArray) throws IOException{
//        System.out.println(replicaSockets);
        checkReplicaSockets();
        for(Socket replicaSocket: replicaSockets){
            PrintWriter writer = new PrintWriter(replicaSocket.getOutputStream(), true);
            for(String command: commandArray){
                writer.print(command+"\r\n");
            }
            writer.flush();
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

    private void addReplica(Socket replicaSocket) throws IOException{
        // wait for following commands
        // wait for 1s if no command is received
        ArrayList<String> replconf = readCommand(replicaSocket);
        /*System.out.println(replconf);*/
        handleCommand(replconf, replicaSocket);
        System.out.println(replconf);
        if (!(replconf.size()==7) || !(replconf.get(2).equalsIgnoreCase("REPLCONF") && replconf.get(4).equalsIgnoreCase("listening-port"))) {
            return;
        }
        ArrayList<String> replconf2= readCommand(replicaSocket);
        /*System.out.println(replconf2);*/
        handleCommand(replconf2, replicaSocket);
        if(!(replconf2.size()==7) || !(replconf2.get(2).equalsIgnoreCase("REPLCONF") && replconf2.get(4).equalsIgnoreCase("capa")
                && replconf2.get(6).equalsIgnoreCase("psync2"))){
            return;
        }
        ArrayList<String> psync=readCommand(replicaSocket);
        handleCommand(psync, replicaSocket);
        System.out.println(psync);
        if(!psync.get(2).equalsIgnoreCase("PSYNC")){
            return;
        }
        System.out.println("Replica added");
        replicaSockets.add(replicaSocket);
        System.out.println(replicaSockets);
        startRepliThread(replicaSocket);
    }

    public void startRepliThread(Socket replicaSocket) throws IOException{
        new Thread(() -> {
            try {
                listenToSocketCommand(replicaSocket);
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
                e.printStackTrace();
            }
        }).start();
    }

    public void checkReplicaSockets(){
        Iterator<Socket> iterator = replicaSockets.iterator();
        while (iterator.hasNext()) {
            Socket replicaSocket = iterator.next();
            try {
                PrintWriter writer = new PrintWriter(replicaSocket.getOutputStream(), true);
                writer.print("*1\r\n$4\r\nping\r\n");
                writer.flush();
                // receive pong
                String pong = readMessage(replicaSocket);
            } catch (IOException e) {
                replicaSockets.remove(replicaSocket);
            }
        }
    }
}
