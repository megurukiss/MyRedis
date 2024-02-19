import org.apache.commons.cli.*;

public class RedisStarter {

    private static void parseAndStart(Options options,String[] args){
        RedisServer server=null;
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
            if(cmd.hasOption("replicaof")){
                server=new SlaveServer();
                server.setRole("slave");
                String[] replicaof = cmd.getOptionValues("replicaof");
                if(replicaof[0].equals("localhost")) {
                    ((SlaveServer) server).setMasterIp("127.0.0.1");
                }
                else {
                    ((SlaveServer) server).setMasterIp(replicaof[0]);
                }
                ((SlaveServer) server).setMasterPort(Integer.parseInt(replicaof[1]));
            }else{
                server=new MasterServer();
                server.setRole("master");
            }
            if(cmd.hasOption("port")){
                server.setPort(Integer.parseInt(cmd.getOptionValue("port")));
            }
            if(cmd.hasOption("dir")){
                server.setDir(cmd.getOptionValue("dir"));
            }
            if(cmd.hasOption("dbfilename")){
                server.setDbfilename(cmd.getOptionValue("dbfilename"));
            }
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage());
            // Optionally, print usage information
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Redis", options);
            System.exit(1);
        }

        server.startServer();
        if(server instanceof SlaveServer){
            ((SlaveServer) server).connectToMaster();
        }
    }

    public static void main(String[] args){

        Options options = new Options();
        Option option = Option.builder("p")
                .longOpt("port")
                .numberOfArgs(1)
                .desc("Change the port of Redis")
                .build();
        options.addOption(option);

        option = Option.builder("repl")
                .longOpt("replicaof")
                .numberOfArgs(2)
                .desc("Set the server as a slave of the server at the given IP and port")
                .build();
        options.addOption(option);

        // add dir
        option = Option.builder("d")
                .longOpt("dir")
                .numberOfArgs(1)
                .desc("Set the directory for the rdb files")
                .build();

        // add dbfilename
        option = Option.builder("db")
                .longOpt("dbfilename")
                .numberOfArgs(1)
                .desc("Set the name of the rdb file")
                .build();


        parseAndStart(options,args);
    }
}
