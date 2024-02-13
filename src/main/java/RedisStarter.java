import org.apache.commons.cli.*;

public class RedisStarter {
    public static void main(String[] args){

        RedisServer server=null;
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

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
            if(cmd.hasOption("replicaof")){
                server=new SlaveServer();
                server.setRole("slave");
                String[] replicaof = cmd.getOptionValues("replicaof");
                ((SlaveServer) server).setMasterIp(replicaof[0]);
                ((SlaveServer) server).setMasterPort(Integer.parseInt(replicaof[1]));
            }else{
                server=new RedisServer();
            }
            if(cmd.hasOption("port")){
                server.setPort(Integer.parseInt(cmd.getOptionValue("port")));
            }
        } catch (ParseException e) {
            System.err.println("Error: " + e.getMessage());
            // Optionally, print usage information
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Redis", options);
            System.exit(1);
        }

        server.startServer();

    }
}