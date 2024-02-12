import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

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
              if (line.equalsIgnoreCase("PING")) {
                  writer.print("+PONG\r\n");
                  writer.flush();
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

}
