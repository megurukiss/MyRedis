import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class SlaveServer extends RedisServer{
    String role = "slave";
    String MasterIp;
    int MasterPort;
    int ACK = 0;
    private String SLAVE_REPL_OFFSET = "0";
    Boolean startACKCounting = false;
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
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            new Thread(() -> {
                while (true) {
                    try {
                        Socket clientSocket = serverSocket.accept();
                        new Thread(() -> {
                            // wait for 100ms
                            // wait for master socket to complete task
                            // delete in final version
                            handleClient(clientSocket);
                        }).start();
                    } catch (IOException e) {
                        System.out.println("IOException: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }).start();
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
            e.printStackTrace();
        }
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
                handlePing(clientSocket);
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
//                System.out.println(map);
                break;
            case "get":
                handleGet(commandArray.get(4), writer,3);
                break;
            case "psync":
                handlePsync(writer);
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
            case "config":
                if(commandArray.get(4).equalsIgnoreCase("get") && commandArray.size()>6){
                    handleConfigGet(commandArray.get(6), writer);
                }
                else{
                    writer.print("-ERR unknown subcommand or wrong number of arguments for 'config'\r\n");
                    writer.flush();
                }
                break;
            default:
                writer.print("-ERR unknown command '"+command+"'\r\n");
                writer.flush();
                break;
        }
    }

    public void handlePing(Socket clientSocket) throws IOException{
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
        int remotePort = clientSocket.getPort();
        String remoteIp = clientSocket.getInetAddress().getHostAddress();
        if(remotePort!=MasterPort || !remoteIp.equals(MasterIp)){
            writer.print("+PONG\r\n");
            writer.flush();
        }
    }

    public void handleSet(String key, String value, Socket clientSocket) throws IOException{
        synchronized (map) {
            map.put(key, value);
            int remotePort = clientSocket.getPort();
            String remoteIp = clientSocket.getInetAddress().getHostAddress();
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
            if (remotePort != MasterPort || !remoteIp.equals(MasterIp)) {
                writer.print("+OK\r\n");
                writer.flush();
            }else{
                // add offset
                SLAVE_REPL_OFFSET = String.valueOf(Integer.parseInt(SLAVE_REPL_OFFSET) + 1);
            }
        }
    }

    public void handleSetWithExpiry(String key, String value, String expiry, Socket clientSocket) throws IOException{
        synchronized (map) {
            long time = System.currentTimeMillis();
            long expiryTime = Long.parseLong(expiry);
            map.put(key, value);
            ExpiryMap.put(key, time + expiryTime);
            int remotePort = clientSocket.getPort();
            String remoteIp = clientSocket.getInetAddress().getHostAddress();
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
            if (remotePort != MasterPort || !remoteIp.equals(MasterIp)) {
                writer.print("+OK\r\n");
                writer.flush();
            }else{
                // add offset
                SLAVE_REPL_OFFSET = String.valueOf(Integer.parseInt(SLAVE_REPL_OFFSET) + 1);
            }
        }
    }

    public void handleGet(String key, PrintWriter writer,int countDownLatch) {
        /*System.out.println("Key: "+key);*/
        synchronized (map) {
            String value = map.get(key);
            Long expiryTime = ExpiryMap.get(key);
            if (value != null) {
                if (expiryTime != null && System.currentTimeMillis() > expiryTime) {
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
            else {
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if(countDownLatch>0){
                    handleGet(key,writer,countDownLatch-1);
                }
                else {
                    System.out.println("Key not found");
                    writer.print("$-1\r\n");
                }
            }
            writer.flush();
        }
    }

    private void handlePsync(PrintWriter writer){
        writer.print(String.format("+FULLRESYNC %s\r\n", SLAVE_REPL_OFFSET));
        writer.flush();
    }

    public void handleACK(PrintWriter writer){
        String[] ackCommand = new String[]{"REPLCONF","ACK",String.valueOf(ACK)};
        writer.print(toRESP(ackCommand));
        writer.flush();
        startACKCounting = true;
    }

    public void listenToMaster() throws IOException{
        new Thread(() ->{
            try {
                while (true){
                    listenToSocketCommand(masterSocket);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }
    public void connectToMaster(){
        try{
            Socket tempSocket = new Socket(MasterIp, MasterPort);
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(tempSocket.getOutputStream()));
            String pingCommand= toRESP(new String[]{"PING"});
            String replconfigCommand1= toRESP(new String[]{"REPLCONF","listening-port",String.valueOf(this.port)});
            String replconfigCommand2= toRESP(new String[]{"REPLCONF","capa","psync2"});
            String psyncCommand= toRESP(new String[]{"PSYNC","?","-1"});
            writer.print(pingCommand);
            writer.flush();
            // receive message like +PONG and check if it is +PONG
            checkMessage(readMessage(tempSocket),"+PONG");
            writer.print(replconfigCommand1);
            writer.flush();
            checkMessage(readMessage(tempSocket),"+OK");
            writer.print(replconfigCommand2);
            writer.flush();
            checkMessage(readMessage(tempSocket),"+OK");
            writer.print(psyncCommand);
            writer.flush();
            checkMessageByRegex(readMessage(tempSocket),"^\\+FULLRESYNC ([a-zA-Z0-9]+) ([0-9]+)$");
            byte[] fileContent= readRDBFile(tempSocket);

            masterSocket = tempSocket;
            listenToMaster();
        }catch (IOException e){
            e.printStackTrace();
        }
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
                while ((ch = is.read()) != -1) {
                    if (ch == '\r') { // Expecting \r\n sequence
                        if ((ch = is.read()) == '\n') {
                            break;
                        } else {
                            throw new IOException("Malformed length terminator");
                        }
                    }
                    sb.append((char) ch);
                }
                int arrayLength = Integer.parseInt(sb.toString());
                // create byte array to store file content
                byte[] fileContent = new byte[arrayLength];
                // read file content
                int bytesRead = 0;
                while (bytesRead < arrayLength) {
                    int result = is.read(fileContent, bytesRead, arrayLength - bytesRead);
                    if (result == -1) {
                        throw new IOException("Error reading file content; stream ended prematurely");
                    }
                    bytesRead += result;
                }
                return fileContent;
            } else{
                throw new IOException("Not a valid message");
            }
        }
        throw new IOException("Error reading file content");
    }

    public static int countCommandLength(ArrayList<String> command){
        int count=0;
        for(String s: command){
            count+=s.length();
            count+=2; // for \r\n
        }
        return count;
    }

    @Override
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
                    if(startACKCounting){
                        ACK+=countCommandLength(commandArray);
                    }
                    handleCommand(commandArray, replicaSocket);
                    commandArray.clear();
                }
            }catch (Exception e){
                commandArray.clear();
                System.out.println("Exception: "+e.getMessage());
            }
        }
    }
}
