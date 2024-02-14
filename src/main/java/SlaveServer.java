import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class SlaveServer extends RedisServer{
    String role = "slave";
    String MasterIp;
    int MasterPort;
    Socket masterSocket=null;
    public SlaveServer(){
        super();
    }
    public void setMasterIp(String MasterIp) {
        this.MasterIp = MasterIp;
    }
    public void setMasterPort(int MasterPort) {
        this.MasterPort = MasterPort;
    }

    @Override
    public void startServer() {
        try {
            listenToMaster();
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

    public void listenToMaster() throws IOException{
        new Thread(() ->{
            try {
                InputStream is = masterSocket.getInputStream();
                int ch;
                while((ch = is.read()) != -1){
                    if(ch=='*'){
                        // read array length
                        int nxtChar= is.read()-48;
                        // convert ascii to integer
                        int arrayLength = nxtChar*2;
                        // initialize array to store command
                        ArrayList<String> commandArray = new ArrayList<>();
                        commandArray.add("*"+nxtChar);
                        is.read();
                        is.read();
                        // read array elements
                        while(arrayLength>0){
                            StringBuilder sb = new StringBuilder();
                            while((ch = is.read()) != -1){
                                if(ch=='\r'){
                                    break;
                                }
                                sb.append((char)ch);
                            }
                            is.read();
                            commandArray.add(sb.toString());
                            arrayLength--;
                        }
                        handleCommand(commandArray, masterSocket);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    @Override
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
                    handleSet(commandArray.get(4), commandArray.get(6), clientSocket);
                }else if(commandLength==5){
                    if(commandArray.get(8).equalsIgnoreCase("px")){
                        handleSetWithExpiry(commandArray.get(4), commandArray.get(6), commandArray.get(10), clientSocket);
                    }
                }else{
                    writer.print("-ERR syntax error\r\n");
                    writer.flush();
                }
                break;
            case "get":
                System.out.println(commandArray);
                System.out.println(map.toString());
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


    public void handleSet(String key, String value, Socket clientSocket) throws IOException{
        map.put(key, value);
        int remotePort = clientSocket.getPort();
        String remoteIp = clientSocket.getInetAddress().getHostAddress();
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
        if(remotePort!=MasterPort || !remoteIp.equals(MasterIp)){
            writer.print("+OK\r\n");
            writer.flush();
        }
    }

    public void handleSetWithExpiry(String key, String value, String expiry, Socket clientSocket) throws IOException{
        long time = System.currentTimeMillis();
        long expiryTime = Long.parseLong(expiry);
        map.put(key, value);
        ExpiryMap.put(key, time+expiryTime);
        int remotePort = clientSocket.getPort();
        String remoteIp = clientSocket.getInetAddress().getHostAddress();
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
        if(remotePort!=MasterPort || !remoteIp.equals(MasterIp)){
            writer.print("+OK\r\n");
            writer.flush();
        }
    }
    public void connectToMaster(){
        try{
            masterSocket = new Socket(MasterIp, MasterPort);
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(masterSocket.getOutputStream()));
            String pingCommand= toRESP(new String[]{"PING"});
            String replconfigCommand1= toRESP(new String[]{"REPLCONF","listening-port",String.valueOf(MasterPort)});
            String replconfigCommand2= toRESP(new String[]{"REPLCONF","capa","psync2"});
            String psyncCommand= toRESP(new String[]{"PSYNC","?","-1"});
            writer.print(pingCommand);
            writer.print(replconfigCommand1);
            writer.print(replconfigCommand2);
            writer.print(psyncCommand);
            writer.flush();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
