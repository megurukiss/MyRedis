

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");
    RedisServer server = new RedisServer();
    setPort(args, server);
    server.startServer();
  }

  public static void setPort(String[] args, RedisServer server){
    for(int i=0; i<args.length; i++){
      if(args[i].equals("--port") && i+1<args.length){
        try{
          server.setPort(Integer.parseInt(args[i+1]));
        }catch (NumberFormatException e){
          System.out.println("Invalid port number");
          System.exit(1);
        }
        break;
      }
    }
  }
}
