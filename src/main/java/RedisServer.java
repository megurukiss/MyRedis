import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class RedisServer {
    ServerSocket serverSocket = null;
    int port = 6379;
    String role = "master";
    ConcurrentHashMap<String, String> map;
    ConcurrentHashMap<String,Long> ExpiryMap;

    public RedisServer() {
        map= new ConcurrentHashMap<>();
        ExpiryMap = new ConcurrentHashMap<>();
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public ServerSocket getServerSocket() {
        return serverSocket;
    }

    public int getPort() {
        return port;
    }

    public void startServer() {
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            /*while(true){
                Socket clientSocket = serverSocket.accept();
                new Thread(() ->{
                    handleClient(clientSocket);
                }).start();
            }*/
            // start a thread for serverSocket
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
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    public void handleClient(Socket clientSocket) {
        try {
            listenToSocketCommand(clientSocket);
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }

    public void handleCommand(ArrayList<String> commandArray, Socket clientSocket) throws IOException{
//        String[] commandArray = splitCommand(command);
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

    public void handlePing(PrintWriter writer){
        writer.print("+PONG\r\n");
        writer.flush();
    }

    public void handleEcho(String message, PrintWriter writer){
        writer.print("$"+message.length()+"\r\n"+message+"\r\n");
        writer.flush();
    }

    public void handleSet(String key, String value, PrintWriter writer){
        map.put(key, value);
        writer.print("+OK\r\n");
        writer.flush();
    }

    public void handleSetWithExpiry(String key, String value, String expiry, PrintWriter writer){
        long time = System.currentTimeMillis();
        long expiryTime = Long.parseLong(expiry);
        map.put(key, value);
        ExpiryMap.put(key, time+expiryTime);
        writer.print("+OK\r\n");
        writer.flush();
    }

    public void handleGet(String key, PrintWriter writer){
        /*System.out.println("Key: "+key);*/
        String value = map.get(key);
        Long expiryTime = ExpiryMap.get(key);
        if(value!=null){
            if(expiryTime!=null && System.currentTimeMillis()>expiryTime) {
                map.remove(key);
                ExpiryMap.remove(key);
                System.out.println("Key expired");
                writer.print("$-1\r\n");
                writer.flush();
                return;
            }
            else {
                System.out.println("Key found");
                writer.print("$" + value.length() + "\r\n" + value + "\r\n");
            }
        }
        else{
            System.out.println("Key not found");
            writer.print("$-1\r\n");
        }
        writer.flush();
    }

    public void handleInfo(PrintWriter writer){
        writer.print(bulkString("role:"+role));
        writer.flush();
    }
    public void handleWait(Socket clientSocket) throws IOException{
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
        writer.print(":0\r\n");
        writer.flush();
    }

    public static String bulkString(String message){
        return "$"+message.length()+"\r\n"+message+"\r\n";
    }

    public static String[] splitCommand(String command) {
        String[] commandArray = command.split("\\r\\n");
//      System.out.println(Arrays.toString(commandArray));
        return commandArray;
    }
    public static String toRESP(String[] commandArray){
        StringBuilder response = new StringBuilder();
        response.append("*"+commandArray.length+"\r\n");
        for(String s:commandArray){
            response.append("$"+s.length()+"\r\n"+s+"\r\n");
        }
        return response.toString();
    }

    public void listenToSocketCommand(Socket replicaSocket) throws IOException{
        BufferedReader reader=new BufferedReader(new InputStreamReader(replicaSocket.getInputStream()));
        String line;
        ArrayList<String> commandArray = new ArrayList<>();
        int commandLength=0;
        while ((line = reader.readLine()) != null) {
            try{
                commandArray.add(line);
                if (commandArray.size() == 1) {
                    commandLength = Integer.parseInt(commandArray.getFirst().substring(1)) * 2 + 1;
                } else if (commandArray.size() == commandLength) {
                    System.out.println(commandArray);
                    handleCommand(commandArray, replicaSocket);
                    commandArray.clear();
                }
            }catch (Exception e){
                commandArray.clear();
                System.out.println("Exception: "+e.getMessage());
            }
        }
    }
    public static ArrayList<String> readCommand(Socket socket) throws IOException{
        InputStream is = socket.getInputStream();
        int ch;
        while((ch = is.read()) != -1){
            // skip starting \r\n
            if(ch=='\r' || ch=='\n'){
                continue;
            }
            if(ch=='*'){
                // read array length until \r\n
                StringBuilder lengthString= new StringBuilder();
                while ((ch = is.read()) != -1) {
                    if (ch == '\r') { // Expect \r\n as line terminator
                        if ((ch = is.read()) == '\n') {
                            break;
                        } else {
                            throw new IOException("Malformed command: Expected '\\n' after '\\r'");
                        }
                    }
                    lengthString.append((char) ch);
                }
                int halfLength = Integer.parseInt(lengthString.toString());
                // convert ascii to integer
                int arrayLength = halfLength*2;
                // initialize array to store command
                ArrayList<String> commandArray = new ArrayList<>();
                commandArray.add("*"+halfLength);
                // read array elements
                while(arrayLength>0){
                    StringBuilder sb = new StringBuilder();
                    while((ch = is.read()) != -1){
                        if (ch == '\r') {
                            if ((ch = is.read()) == '\n') {
                                break;
                            } else {
                                throw new IOException("Malformed command: Expected '\\n' after '\\r'");
                            }
                        }
                        sb.append((char)ch);
                    }
                    commandArray.add(sb.toString());
                    arrayLength--;
                }
                return commandArray;
            }else{
                throw new IOException("Not a valid command");
            }
        }
        throw new IOException("Error reading command");
    }

    public static ArrayList<String> readCommandByLine(Socket socket) throws IOException{
        BufferedReader reader=new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String line;
        ArrayList<String> commandArray = new ArrayList<>();
        int commandLength=0;
        while ((line = reader.readLine()) != null) {
            commandArray.add(line);
            if (commandArray.size() == 1) {
                commandLength = Integer.parseInt(commandArray.getFirst().substring(1)) * 2 + 1;
            } else if (commandArray.size() == commandLength) {
                return commandArray;
            }
        }
        throw new IOException("Error reading command");
    }

    public static String readMessage(Socket socket) throws IOException{
        InputStream is = socket.getInputStream();
        int ch;
        StringBuilder sb = new StringBuilder();
        Boolean messageStarted = false;
        while((ch = is.read()) != -1){
            // skip starting \r\n
            if(!messageStarted && (ch=='\r' || ch=='\n')){
                continue;
            }
            if(ch=='+'){
                messageStarted = true;
                sb.append((char)ch);
                continue;
            }
            if (messageStarted) {
                if (ch == '\r' || ch == '\n') {
                    break; // End of message
                }
                sb.append((char) ch);
            } else{
                throw new IOException("Not a valid message");
            }
        }

        if (sb.isEmpty()) {
            throw new IOException("Empty message or connection closed");
        }

//        System.out.println(sb.toString());
        return sb.toString();
    }

}
