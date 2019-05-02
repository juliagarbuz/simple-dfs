import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import java.lang.management.ThreadInfo;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.io.*;
import java.nio.file.*;
import java.time.*;

import java.lang.Object;
import java.nio.charset.StandardCharsets;

public class DFSNodeInstance {

    private static final String PATH_TO_ROOT = "../../../";
    private static final String DATA_DIRECTORY = "data/";

    private static final String PATH_TO_RESOURCES = "../resources/";
    private static final String DFS_PROPERTIES_FILE = "dfs.properties";
    private static final String DFS_DEFAULTS_FILE = "default.properties";

    private static int update_frequency;

    private static NodeInfo nodeInfo;
    private static NodeInfo coordinatorInfo;
    private static CoordinatorConfigurationManager coordinatorConfigManager;

    private static DFSNodeHandler dfsNodeHandler;
    private static DFSNode.Processor processor;

    public static String getHostName() {
        try {
            InetAddress localhost = InetAddress.getLocalHost();
            return localhost.getHostName();
        } catch (UnknownHostException ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public static void readCoordinatorConfigsFromFile(boolean getIpPort) {
      // [1] Read in DEFAULT properties (from file)
      String defaultPropertiesFilePath = PATH_TO_RESOURCES + DFS_DEFAULTS_FILE;
      Properties defaultCoordinatorProperties = new Properties();
      try {
          defaultCoordinatorProperties.load(new FileInputStream(defaultPropertiesFilePath));
      } catch (Exception e) {
        e.printStackTrace();
      }

      // [2] Read in USER DEFINED properties (from file)
      String userPropertiesFilePath = PATH_TO_RESOURCES + DFS_PROPERTIES_FILE;
      Properties userDefinedProperties = new Properties(defaultCoordinatorProperties);
      try {
          userDefinedProperties.load(new FileInputStream(userPropertiesFilePath));
      } catch (Exception e) {
        e.printStackTrace();
      }

      // [3] Get needed information
      CoordinatorStatus currentCoordinatorStatus =
        CoordinatorStatus.valueOf(userDefinedProperties.getProperty(Property.status.name()));
      if (currentCoordinatorStatus != CoordinatorStatus.LIVE) {
        System.err.printf("[NodeInstance] Coordinator node is not yet live. Please make sure you started one and try again.\n");
        System.exit(1);
      } else {
        if (getIpPort) {
          coordinatorInfo.ip = userDefinedProperties.getProperty(Property.ip.name());
          coordinatorInfo.port = Integer.valueOf(userDefinedProperties.getProperty(Property.port.name()));
          coordinatorInfo.isCoordinator = true;
        }
      }
      update_frequency = Integer.valueOf(userDefinedProperties.getProperty(Property.update_frequency.name()));
    }

    public static void makeNodeDirectory() {
      String dir_name = nodeInfo.ip + ":" + Integer.toString(nodeInfo.port);
      try {
        Files.createDirectories(Paths.get(PATH_TO_ROOT + DATA_DIRECTORY + dir_name));
      } catch (IOException e) {
        System.out.println("Unable to create data director(ies).");
        System.exit(1);
      }
    }

    public static void setupThreadedServer() {
        try {
            TServerTransport transport = new TServerSocket(nodeInfo.port);
            TTransportFactory factory = new TFramedTransport.Factory();

            TThreadPoolServer.Args args = new TThreadPoolServer.Args(transport);
            args.processor(processor);
            args.transportFactory(factory);

            TServer server = new TThreadPoolServer(args);

            System.out.printf("[NodeInstance] Starting on '%s:%d'\n",
                    nodeInfo.ip,
                    nodeInfo.port);
            server.serve();

        } catch (TTransportException e) {
            System.out.println(e.getMessage());
            System.out.println("[NodeInstance] Caught TTransportException; is a Node already running?");
        }
    }

    public static void tryToJoinDFS() {

        // Only need to 'join' if not coordinator:
        if (!nodeInfo.isCoordinator) {

          // Connect to [COORDINATOR NODE]
          TTransport transport = new TSocket(coordinatorInfo.ip, coordinatorInfo.port);
          TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
          DFSNode.Client coordinatorNodeClient = new DFSNode.Client(protocol);

          try {
            transport.open();
            Response joinResponse = coordinatorNodeClient.joinDFS(nodeInfo);
            transport.close();

            if (joinResponse.acknowledgement == Acknowledgement.SUCCESS) {
              System.out.println("[NodeInstance] Successfully joined DFS");
            } else {
              System.out.println("[NodeInstance] Failed to join DFS (" + joinResponse.message + ")");
              System.exit(1);
            }

          }
          catch (TException e) {
            System.err.println("[NodeInstance] Error joining to DFS (connecting to coordinator node)\n");
            e.printStackTrace();
            System.exit(1);
          }

        }
    }

    public static void callUpdate() {

      TTransport transport = new TSocket(nodeInfo.ip, nodeInfo.port);
      TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
      DFSNode.Client coordinatorNodeClient = new DFSNode.Client(protocol);

      try {
        transport.open();
        Response updateResponse = coordinatorNodeClient.update();
        transport.close();

        if (updateResponse.acknowledgement == Acknowledgement.SUCCESS) {
          System.out.println("[NodeInstance] Successfully updated local files.");
        } else {
          System.out.println("[NodeInstance] Failed to update local files (" + updateResponse.message + ").");
        }

      }
      catch (TException e) {
        System.err.println("[NodeInstance] Error updating files (while trying to connect to coordinator node)\n");
        e.printStackTrace();
      }
    }

    public static void main(String[] args) {

      /**********************************************************************
      [PHASE 1]: CONFIGURE NODE FROM ARGS, DEFAULTS, and CONFIGS
      **********************************************************************/

      // INIT NODE INFO OBJECT:
      nodeInfo = new NodeInfo();

      // GET HOST OF CURRENT NODE
      nodeInfo.ip = getHostName();
      if (nodeInfo.ip == null) {
          System.err.println("[NodeInstance] UNABLE TO RECOGNIZE HOST\n");
          System.exit(1);
      }

      if (args.length >= 1) {
        // Must have at least one arg to be valid

        if (args[0].equals("-c")) {
          coordinatorConfigManager = new CoordinatorConfigurationManager(nodeInfo);

          // Slice off '-c' arg and join args via '\n' to resemble property file
          List<String> commandLineArgs = Arrays.asList(args);
          ArrayList<String> slicedCommandLineArgs = new ArrayList<String>(commandLineArgs.subList(1, commandLineArgs.size()));
          String commandLineArgsAsString = String.join("\n", slicedCommandLineArgs);

          coordinatorConfigManager.readInCoordinatorProperties(commandLineArgsAsString);
          update_frequency = coordinatorConfigManager.getUpdateFrequency();
          System.out.print("[NodeInstance] Coordinator node configurations PRIOR to adjustment:");
          coordinatorConfigManager.printCoordinatorConfiguration();
          coordinatorConfigManager.setNrNw();
          System.out.print("[NodeInstance] Coordinator node configurations POST adjustment:");
          coordinatorConfigManager.printCoordinatorConfiguration();
          CoordinatorConfiguration coordinatorConfig = coordinatorConfigManager.getCoordinatorConfiguration();
          nodeInfo = coordinatorConfigManager.getCoordinatorNodeInfo();

          coordinatorConfigManager.writeCoordinatorInfoToPropFile();
          dfsNodeHandler = new DFSNodeHandler(nodeInfo, coordinatorConfig);
          processor = new DFSNode.Processor(dfsNodeHandler);

        } else {
            /******************************************************************
            [1B] CONFIGURE NODE INSTANCE as a [REGULAR NODE]:
            ******************************************************************/
            nodeInfo.isCoordinator = false;

            // INIT COORDINATOR NODE INFO OBJECT:
            coordinatorInfo = new NodeInfo();
            coordinatorInfo.isCoordinator = true;

            if (args.length == 1) {
              // [REGULAR NODE] [1 ARG] --> Use port from arg and read coordinator info from file
              nodeInfo.port = Integer.valueOf(args[0]);
              readCoordinatorConfigsFromFile(true);

            } else if (args.length == 2) {
              // [REGULAR NODE] [2 ARGS] --> Use port and coordinator info from args
              nodeInfo.port = Integer.valueOf(args[0]);
              String[] coordIpAndPort = args[1].split(":");
              coordinatorInfo.ip = coordIpAndPort[0];
              coordinatorInfo.port = Integer.valueOf(coordIpAndPort[1]);
              readCoordinatorConfigsFromFile(false);

            } else {
              // (More than 2 args)
              System.err.printf("[NodeInstance] UNKNOWN NUMBER OF ARGUMENTS (%d). EXPECTING 1 OR 2.", args.length);
              System.exit(1);

            }

            // Create DFSNodeHandler/processor using regular node constructor
            dfsNodeHandler = new DFSNodeHandler(nodeInfo, coordinatorInfo);
            processor = new DFSNode.Processor(dfsNodeHandler);

            /******************************************************************/
          }

        } else {
          /******************************************************************
          [1C] UNABLE TO CONFIG WITHOUT ARGS
          ******************************************************************/

          // No args --> Error
          System.err.printf("[NodeInstance] EXPECTING AT LEAST 1 ARGUMENT. (%d) PROVIDED.\n", args.length);
          System.exit(1);
        }

        /**********************************************************************
        [PHASE 2]: MAKE NODE'S DATA DIRECTORY
        **********************************************************************/

        makeNodeDirectory();

        /**********************************************************************
        [PHASE 3]: Set up thread processes and run to start node
        **********************************************************************/

        Runnable initiateJoin = new Runnable() {
            @Override
            public void run() {
                tryToJoinDFS();
            }
        };

        Runnable shutdown_hook = new Runnable() {
          @Override
          public void run() {
            // Added shutdown hook to catch ^C
            final long start = System.nanoTime();
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                public void run() {
                    System.out.format("\n[NodeInstance] Shutting down DFSNode.\n");
                    if (nodeInfo.isCoordinator) {
                      coordinatorConfigManager.cleanCoordinatorPropFile();
                      System.out.println("[NodeInstance] Resetting Coordinator Node's properties.");
                    }
                }
            }));
            while(true) {
                try {
                  Thread.sleep(500);
                } catch (InterruptedException ie) {
                  ie.printStackTrace();
                }
            }
          }
        };

        Runnable update = new Runnable() {
            @Override
            public void run() {
              int counter = 1;
              while(true) {
                try {
                  Thread.sleep(update_frequency);
                  System.out.printf("[NodeInstance] INITIATED UPDATE #%d\n", counter);
                  callUpdate();
                  System.out.printf("[NodeInstance] FINISHED UPDATE #%d\n", counter);
                  counter++;
                } catch (InterruptedException ie) {
                  ie.printStackTrace();
                }
              }
            }
        };

        Thread t_shutdown= new Thread(shutdown_hook);
        t_shutdown.start();

        Thread t_join= new Thread(initiateJoin);
        t_join.start();

        Thread t_update= new Thread(update);
        t_update.start();

        setupThreadedServer();

    }
}
