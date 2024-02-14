import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.regex.Pattern;

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
                        //
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
            case "replconf":
                if(commandArray.get(4).equalsIgnoreCase("GETACK")){
                    handleACK(writer);
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

    public void handleACK(PrintWriter writer){
        String[] ackCommand = new String[]{"REPLCONF","ACK",String.valueOf(0)};
        writer.print(toRESP(ackCommand));
        writer.flush();
    }
    public void connectToMaster(){
        try{
            Socket tempSocket = new Socket(MasterIp, MasterPort);
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(tempSocket.getOutputStream()));
            String pingCommand= toRESP(new String[]{"PING"});
            String replconfigCommand1= toRESP(new String[]{"REPLCONF","listening-port",String.valueOf(MasterPort)});
            String replconfigCommand2= toRESP(new String[]{"REPLCONF","capa","psync2"});
            String psyncCommand= toRESP(new String[]{"PSYNC","?","-1"});
            writer.print(pingCommand);
            // receive message like +PONG and check if it is +PONG
           checkMessage(readMessage(tempSocket),"+PONG");
            writer.print(replconfigCommand1);
            checkMessage(readMessage(tempSocket),"+OK");
            writer.print(replconfigCommand2);
            checkMessage(readMessage(tempSocket),"+OK");
            writer.print(psyncCommand);
            checkMessageByRegex(readMessage(tempSocket),"^\\+FULLRESYNC ([a-fA-F0-9]+) ([0-9]+)$");
            readMessage(tempSocket);
            byte[] fileContent= readRDBFile(tempSocket);

            masterSocket = tempSocket;
            writer.flush();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static String readMessage(Socket socket) throws IOException{
        InputStream is = socket.getInputStream();
        int ch;
        StringBuilder sb = new StringBuilder();
        while((ch = is.read()) != -1){
            // skip starting \r\n
            if(ch=='\r' || ch=='\n'){
                continue;
            }
            if(ch=='+'){
                sb.append((char)ch);
                while((ch = is.read()) != -1){
                    if(ch=='\r' || ch=='\n'){
                        break;
                    }
                    sb.append((char)ch);
                }
                break;
            }
            else{
                throw new IOException("Not a valid message");
            }
        }
        System.out.println(sb.toString());
        return sb.toString();
    }

    public static void checkMessage(String message, String expected) throws IOException{
        if(message.equals(expected)){
            return;
        }
        throw new IOException("Connection to master failed");
    }

    public static void checkMessageByRegex(String message,String regex) throws IOException{
        Pattern pattern = Pattern.compile(regex);
        if(!pattern.matcher(message).matches()){
            throw new IOException("Connection to master failed");
        }
    }

    public static byte[] readRDBFile(Socket socket) throws IOException{
        InputStream is = socket.getInputStream();
        int ch;
        while((ch=is.read())!=-1){
            // skip heading \r\n
            if(ch=='\r' || ch=='\n'){
                continue;
            }
            if(ch=='$'){
                // read array length until \r\n
                StringBuilder sb= new StringBuilder();
                while((ch=is.read())!=-1){
                    if(ch=='\r' || ch=='\n'){
                        break;
                    }
                    sb.append((char)ch);
                }
                int arrayLength = Integer.parseInt(sb.toString());
                // create byte array to store file content
                byte[] fileContent = new byte[arrayLength];
                // read file content
                int label= is.read(fileContent,0,arrayLength);
                if(label==-1){
                    throw new IOException("Error reading file content");
                }
                return fileContent;
            }
            else{
                throw new IOException("Not a valid message");
            }
        }
        throw new IOException("Error reading file content");
    }

    public ArrayList<String> readCommand(Socket socket) throws IOException{
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
                while((ch=is.read())!=-1){
                    if(ch=='\r' || ch=='\n'){
                        break;
                    }
                    lengthString.append((char)ch);
                }
                int halfLength = Integer.parseInt(lengthString.toString());
                // convert ascii to integer
                int arrayLength = halfLength*2;
                // initialize array to store command
                ArrayList<String> commandArray = new ArrayList<>();
                commandArray.add("*"+halfLength);
                // skip \r\n
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
                return commandArray;
            }else{
                throw new IOException("Not a valid command");
            }
        }
        throw new IOException("Error reading command");
    }

}
