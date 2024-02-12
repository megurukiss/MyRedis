import java.io.*;
import java.lang.reflect.Array;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    //  Uncomment this block to pass the first stage
        ServerSocket serverSocket = null;
        int port = 6379;
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

  public static void handleClient(Socket clientSocket) {
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
              }
              commandArray.clear();
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

  public static void handleCommand(ArrayList<String> commandArray, PrintWriter writer){
//        String[] commandArray = splitCommand(command);
        int commandLength = Integer.parseInt(commandArray.getFirst().substring(1));
        assert commandLength==1 || commandLength==2;
        switch (commandLength){
            case 1:
                if(commandArray.get(2).equalsIgnoreCase("PING")){
//                    System.out.println(Arrays.toString(commandArray));
                    handlePing(writer);
                }
                break;
            case 2:
                if(commandArray.get(2).equalsIgnoreCase("ECHO")){
//                    System.out.println(Arrays.toString(commandArray));
                    handleEcho(commandArray.get(4), writer);
                }
                break;
            default:
                break;
        }
  }

    public static void handlePing(PrintWriter writer){
        writer.print("+PONG\r\n");
        writer.flush();
    }

    public static void handleEcho(String message, PrintWriter writer){
        writer.print("$"+message.length()+"\r\n"+message+"\r\n");
        writer.flush();
    }

  public static String[] splitCommand(String command) {
      String[] commandArray = command.split("\\r\\n");
//      System.out.println(Arrays.toString(commandArray));
    return commandArray;
  }

}
