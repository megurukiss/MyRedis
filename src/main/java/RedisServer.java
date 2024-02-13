import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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


    public void startServer() {
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            while(true){
                Socket clientSocket = serverSocket.accept();
                new Thread(() ->{
                    handleClient(clientSocket);
                }).start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    public void handleClient(Socket clientSocket) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
            String line;
            ArrayList<String> commandArray = new ArrayList<>();
            int commandLength=0;
            while ((line = reader.readLine()) != null) {
                commandArray.add(line);
                if(commandArray.size()==1){
                    commandLength = Integer.parseInt(commandArray.getFirst().substring(1))*2+1;
                }
                else if(commandArray.size()==commandLength){
                    handleCommand(commandArray, writer);
                    commandArray.clear();
                }
            }
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

    public void handleCommand(ArrayList<String> commandArray, PrintWriter writer){
//        String[] commandArray = splitCommand(command);
        int commandLength = Integer.parseInt(commandArray.getFirst().substring(1));
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
        String value = map.get(key);
        Long expiryTime = ExpiryMap.get(key);
        System.out.println(expiryTime);
        if(value!=null){
            if(expiryTime!=null && System.currentTimeMillis()>expiryTime) {
                map.remove(key);
                ExpiryMap.remove(key);
                writer.print("$-1\r\n");
                writer.flush();
                return;
            }
            else {
                writer.print("$" + value.length() + "\r\n" + value + "\r\n");
            }
        }
        else{
            writer.print("$-1\r\n");
        }
        writer.flush();
    }

    public void handleInfo(PrintWriter writer){
        writer.print(bulkString("role:"+role));
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
}
