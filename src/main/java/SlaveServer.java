import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

public class SlaveServer extends RedisServer{
    String role = "slave";
    String MasterIp;
    int MasterPort;
    public SlaveServer(){
        super();
    }
    public void setMasterIp(String MasterIp) {
        this.MasterIp = MasterIp;
    }
    public void setMasterPort(int MasterPort) {
        this.MasterPort = MasterPort;
    }

    public void connectToMaster(){
        try{
            Socket masterSocket = new Socket(MasterIp, MasterPort);
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(masterSocket.getOutputStream()));
            String pingCommand= toRESP(new String[]{"PING"});
            String replconfigCommand1= toRESP(new String[]{"REPLCONF","listening-port",String.valueOf(MasterPort)});
            String replconfigCommand2= toRESP(new String[]{"REPLCONF","capa","psync2"});
            writer.print(pingCommand);
            writer.print(replconfigCommand1);
            writer.print(replconfigCommand2);
            writer.flush();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
