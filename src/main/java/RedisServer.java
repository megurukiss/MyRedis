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
    ConcurrentHashMap<String, String> map;
    public RedisServer() {
        map= new ConcurrentHashMap<>();
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
//        int commandLength = Integer.parseInt(commandArray.getFirst().substring(1));
        String command = commandArray.get(2);
        switch (command.toLowerCase()){
            case "ping":
                handlePing(writer);
                break;
            case "echo":
                handleEcho(commandArray.get(4), writer);
                break;
            case "set":
                handleSet(commandArray.get(4), commandArray.get(6),writer);
                break;
            case "get":
                handleGet(commandArray.get(4), writer);
                break;
            default:
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

    public void handleGet(String key, PrintWriter writer){
        String value = map.get(key);
        if(value!=null){
            writer.print("$"+value.length()+"\r\n"+value+"\r\n");
        }
        else{
            writer.print("$-1\r\n");
        }
        writer.flush();
    }



    public static String[] splitCommand(String command) {
        String[] commandArray = command.split("\\r\\n");
//      System.out.println(Arrays.toString(commandArray));
        return commandArray;
    }
}
