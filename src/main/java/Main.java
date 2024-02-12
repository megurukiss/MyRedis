import java.io.*;
import java.lang.reflect.Array;
import java.net.ServerSocket;
import java.net.Socket;
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
          while ((line = reader.readLine()) != null) {
              handleCommand(line, writer);
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

  public static void handleCommand(String command, PrintWriter writer){
        String[] commandArray = splitCommand(command);
        if (commandArray.length==1 && commandArray[0].equalsIgnoreCase("ping"){
            handlePing(writer);
            return;
        }
        int commandLength = Integer.parseInt(commandArray[0].substring(1));
        assert commandLength==1 || commandLength==2;
        switch (commandLength){
            case 1:
                if(commandArray[0].equalsIgnoreCase("PING")){
//                    System.out.println(Arrays.toString(commandArray));
                    handlePing(writer);
                }
                break;
            case 2:
                if(commandArray[2].equalsIgnoreCase("ECHO")){
//                    System.out.println(Arrays.toString(commandArray));
                    handleEcho(commandArray[4], writer);
                }
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
    return command.split("\\r\\n");
  }

}
