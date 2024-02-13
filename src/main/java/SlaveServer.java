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
}
