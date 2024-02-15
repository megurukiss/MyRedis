import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class ServerListener implements Runnable{

    RedisServer server;

    public ServerListener(RedisServer server) {
        this.server = server;
    }

    @Override
    public void run() {
        ServerSocket serverSocket = server.getServerSocket();
        try {
            while(true){
                Socket clientSocket = serverSocket.accept();
                new Thread(() ->{
                    server.handleClient(clientSocket);
                }).start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
