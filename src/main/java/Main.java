

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");
    RedisServer server = new RedisServer();
    setArguments(args, server);
    server.startServer();
  }

  public static void setArguments(String[] args, RedisServer server){
    for(int i=0; i<args.length; i++){
      if(args[i].equals("--port") && i+1<args.length){
        try{
          server.setPort(Integer.parseInt(args[i+1]));
        }catch (NumberFormatException e){
          System.out.println("Invalid port number");
          System.exit(1);
        }
      }
      if(args[i].equals("--replicaof") && i+2<args.length){
        server.setRole("slave");
        try{
          server.setMasterIp(args[i+1]);
          server.setMasterPort(Integer.parseInt(args[i+2]));
        }catch (NumberFormatException e){
          System.out.println("Invalid port number");
          System.exit(1);
        }
      }
    }
  }
}
