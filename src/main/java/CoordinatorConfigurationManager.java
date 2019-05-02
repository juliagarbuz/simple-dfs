import java.util.*;
import java.io.*;

import java.lang.Object;
import java.nio.charset.StandardCharsets;

class CoordinatorConfigurationManager {

  private static final String PATH_TO_RESOURCES = "../resources/";
  private static final String DFS_PROPERTIES_FILE = "dfs.properties";
  private static final String DFS_DEFAULTS_FILE = "default.properties";

  private Properties defaultCoordinatorProperties; // From default file
  private Properties userDefinedProperties; // From default file and user defined prop file,
  private Properties coordinatorProperties; // From default file, user defined prop file, and command line args

  private CoordinatorConfiguration coordinatorConfig;
  private NodeInfo nodeInfo;

  private int update_frequency;

  public CoordinatorConfigurationManager(NodeInfo info) {
    coordinatorConfig = new CoordinatorConfiguration();
    nodeInfo = info;
    nodeInfo.isCoordinator = true; // just in case
  }

  public CoordinatorConfiguration getCoordinatorConfiguration() {
    return coordinatorConfig;
  }

  public NodeInfo getCoordinatorNodeInfo() {
    return nodeInfo;
  }

  public int getUpdateFrequency() {
    return update_frequency;
  }

  public void printCoordinatorConfiguration() {
    String line = "=================================================================\n";
    String line2 = "-----------------------------------------------------------------\n";
    String s =  "\n" + line +
                "[COORDINATOR NODE CONFIGURATIONS]:\n" +
                line2 +
                "\tRunning on " + nodeInfo.ip + ":" + Integer.toString(nodeInfo.port) + "\n" +
                "\n\t[QUORUM SELECTION]:\t" + coordinatorConfig.quorumSelection + "\n" +
                "\t[N]\t\t\t" + coordinatorConfig.n + "\n" +
                "\t[Nw]\t\t\t" + coordinatorConfig.nw + "\n" +
                "\t[Nr]\t\t\t" + coordinatorConfig.nr + "\n" +
                line;
    System.out.println(s);
  }

  public void writeStartingStatus() {
    String userPropertiesFilePath = PATH_TO_RESOURCES + DFS_PROPERTIES_FILE;
    MyFileReader reader = new MyFileReader(userPropertiesFilePath);
    if (reader.openFile()) {
      reader.readWholeFile();
      ArrayList<String> fileContents = reader.getFileContentsByLine();
      reader.closeFile();
      int i = 0;
      while (i < fileContents.size() && !fileContents.get(i).contains("status")) {
        i++;
      }
      fileContents.set(i, "status=STARTING");
      userDefinedProperties.setProperty(Property.status.name(), CoordinatorStatus.STARTING.name());
      MyFileWriter writer = new MyFileWriter(userPropertiesFilePath);
      if (writer.openFile()) {
        writer.writeLines(fileContents);
      }
    }
  }

  public void writeCoordinatorInfoToPropFile() {
    String userPropertiesFilePath = PATH_TO_RESOURCES + DFS_PROPERTIES_FILE;
    MyFileReader reader = new MyFileReader(userPropertiesFilePath);
    if (reader.openFile()) {
      reader.readWholeFile();
      ArrayList<String> fileContents = reader.getFileContentsByLine();
      reader.closeFile();
      int i = 0;
      int port_idx = -1;
      int status_idx = -1;
      int ip_idx = -1;
      while (i < fileContents.size()) {
        if (fileContents.get(i).contains("status")) {
          status_idx = i;
        } else if (fileContents.get(i).contains("port")) {
          port_idx = i;
        } else if (fileContents.get(i).contains("ip")) {
          ip_idx = i;
        }
        i++;
      }
      if (status_idx != -1) { fileContents.set(status_idx, "status=LIVE"); }
      else { fileContents.add("status=LIVE"); }
      userDefinedProperties.setProperty(Property.status.name(), CoordinatorStatus.LIVE.name());

      if (port_idx != -1) { fileContents.set(port_idx, "port=" + nodeInfo.port); }
      else { fileContents.add("port=" + nodeInfo.port); }

      if (ip_idx != -1) { fileContents.set(ip_idx, "ip=" + nodeInfo.ip); }
      else { fileContents.add("ip=" + nodeInfo.ip); }

      MyFileWriter writer = new MyFileWriter(userPropertiesFilePath);
      if (writer.openFile()) {
        writer.writeLines(fileContents);
      }
    }
  }

  public void cleanCoordinatorPropFile() {
    String userPropertiesFilePath = PATH_TO_RESOURCES + DFS_PROPERTIES_FILE;
    MyFileReader reader = new MyFileReader(userPropertiesFilePath);
    if (reader.openFile()) {
      reader.readWholeFile();
      ArrayList<String> fileContents = reader.getFileContentsByLine();
      reader.closeFile();
      int i = 0;
      int status_idx = -1;
      int ip_idx = -1;
      while (i < fileContents.size()) {
        if (fileContents.get(i).contains("status")) {
          status_idx = i;
        } else if (fileContents.get(i).contains("ip")) {
          ip_idx = i;
        }
        i++;
      }
      if (status_idx != -1) { fileContents.set(status_idx, "status=DOWN"); }
      else { fileContents.add("status=DOWN"); }

      if (ip_idx != -1) { fileContents.set(ip_idx, "# ip ="); }
      else { fileContents.add("# ip ="); }

      MyFileWriter writer = new MyFileWriter(userPropertiesFilePath);
      if (writer.openFile()) {
        writer.writeLines(fileContents);
      }
    }
  }

  public void readInCoordinatorProperties(String commandLineProperties) {
    // [1] Read in DEFAULT properties (from file)
    String defaultPropertiesFilePath = PATH_TO_RESOURCES + DFS_DEFAULTS_FILE;
    defaultCoordinatorProperties = new Properties();
    try {
        defaultCoordinatorProperties.load(new FileInputStream(defaultPropertiesFilePath));
    } catch (Exception e) {
      e.printStackTrace();
    }

    // [2] Read in USER DEFINED properties (from file)
    String userPropertiesFilePath = PATH_TO_RESOURCES + DFS_PROPERTIES_FILE;
    userDefinedProperties = new Properties(defaultCoordinatorProperties);
    try {
        userDefinedProperties.load(new FileInputStream(userPropertiesFilePath));
    } catch (Exception e) {
      e.printStackTrace();
    }

    // CHECK IF A COORDINATOR IS ALREADY RUNNING OR NOT:
    try {
      CoordinatorStatus coordinator_status = CoordinatorStatus.valueOf(userDefinedProperties.getProperty(Property.status.name()));
      if (coordinator_status != CoordinatorStatus.DOWN) {
        System.err.println("[CoordinatorConfigurationManager] A COORDINATOR NODE IS STARTING OR ALREADY RUNNING. PLEASE START THIS SERVER AS A REGULAR NODE.");
        System.exit(1);
      } else {
        writeStartingStatus();
      }
    } catch (IllegalArgumentException e) {
      System.err.printf("[CoordinatorConfigurationManager] CoordinatorStatus enum '" + userDefinedProperties.getProperty(Property.status.name()) + "' not recognized.\n\n");
    }

    // [3] Read in USER DEFINED properties (from command line args)
    InputStream result = new ByteArrayInputStream(commandLineProperties.getBytes(StandardCharsets.UTF_8));
    coordinatorProperties = new Properties(userDefinedProperties);
    try {
        coordinatorProperties.load(result);
    } catch (Exception e) {
      e.printStackTrace();
    }

    update_frequency = Integer.valueOf(coordinatorProperties.getProperty(Property.update_frequency.name()));

    // [4] Save configs/properties to nodeInfo and coordinatorConfig objects:
    nodeInfo.port = Integer.valueOf(coordinatorProperties.getProperty(Property.port.name()));
    try {
      coordinatorConfig.quorumSelection = QuorumSelection.valueOf(coordinatorProperties.getProperty(Property.quorum_selection.name()));
    } catch (IllegalArgumentException e) {
      coordinatorConfig.quorumSelection = QuorumSelection.valueOf(defaultCoordinatorProperties.getProperty(Property.quorum_selection.name()));
      System.err.printf("[CoordinatorConfigurationManager] QuorumSelection enum '" + coordinatorProperties.getProperty(Property.quorum_selection.name()) +
        "' not recognized. Changed to default (%s).\n\n", coordinatorConfig.quorumSelection);
    }
    coordinatorConfig.n = Integer.valueOf(coordinatorProperties.getProperty(Property.n.name()));

    if (coordinatorConfig.quorumSelection == QuorumSelection.USER_CONFIG) {
      if (coordinatorProperties.getProperty(Property.nw.name()) != null) {
        coordinatorConfig.nw = Integer.valueOf(coordinatorProperties.getProperty(Property.nw.name()));
      } else {
        coordinatorConfig.nw = -1;
      }

      if (coordinatorProperties.getProperty(Property.nr.name()) != null) {
        coordinatorConfig.nr = Integer.valueOf(coordinatorProperties.getProperty(Property.nr.name()));
      } else {
        coordinatorConfig.nr = -1;
      }

    } else {
      coordinatorConfig.nw = -1;
      coordinatorConfig.nr = -1;
    }

  }

  public void setNrNw() {
    /*************************************************************************
    [1]: Check validity of N, reset to min (7) if needed:
    *************************************************************************/
    int minimum_n = Integer.valueOf(defaultCoordinatorProperties.getProperty("minimum_n"));
    if (coordinatorConfig.n < minimum_n) {
      System.err.printf("[CoordinatorConfigurationManager] N entered (%d) is less than minumum required number of replicas. Changed to default (%d).\n\n", coordinatorConfig.n, minimum_n);
      coordinatorConfig.n = minimum_n;
    }

    /*************************************************************************
    [2]: Set Nw and Nr based on quorum selection setting:
    *************************************************************************/

    if (coordinatorConfig.quorumSelection == QuorumSelection.USER_CONFIG) {
      /**********************************************************************
        [USER_CONFIG] If violates either condition, set to default 'RANDOM':
      **********************************************************************/
      if ( (coordinatorConfig.nr + coordinatorConfig.nw <= coordinatorConfig.n)
          || (coordinatorConfig.nw <= coordinatorConfig.n/2) ) {
        System.err.printf("[CoordinatorConfigurationManager] Invalid user-entered Nw (%d) and Nr (%d). Changing QuorumSelection from USER_CONFIG to RANDOM.\n\n", coordinatorConfig.nw, coordinatorConfig.nr);
        coordinatorConfig.quorumSelection = QuorumSelection.valueOf(
          defaultCoordinatorProperties.getProperty(Property.quorum_selection.name()));
      }
    }
    // Separate IF so if QuorumSelection changed, can still calculate nw, nr
    if (coordinatorConfig.quorumSelection != QuorumSelection.USER_CONFIG) {
      if (coordinatorConfig.quorumSelection == QuorumSelection.READ_HEAVY) {
        /**********************************************************************
          [READ_HEAVY] Minimize size of read quorum
        **********************************************************************/
        // Set Nw to MAXIMUM possible:
        coordinatorConfig.nw = coordinatorConfig.n;
        // Set Nr to MINIMUM possible:
        coordinatorConfig.nr = 1;

      } else if (coordinatorConfig.quorumSelection == QuorumSelection.WRITE_HEAVY) {
        /**********************************************************************
          [WRITE_HEAVY] Minimize size of write quorum
        **********************************************************************/
        // Set Nw to MINIMUM possible:
        coordinatorConfig.nw = (coordinatorConfig.n/2) + 1;
        // Set Nr to MAXIMIM possible:
        coordinatorConfig.nr = coordinatorConfig.n;

      } else if (coordinatorConfig.quorumSelection == QuorumSelection.RANDOM) {
        /**********************************************************************
          [RANDOM] Select random Nw and Nr within valid range
        **********************************************************************/
        Random r = new Random();

        // Randomly select an Nw within valid range:
        int nw_min = (coordinatorConfig.n/2) + 1;
        int nw_range = coordinatorConfig.n - nw_min;
        coordinatorConfig.nw = nw_min + r.nextInt(nw_range);

        // Randomly select an Nr within valid range:
        int nr_min = (coordinatorConfig.n - coordinatorConfig.nw) + 1;
        int nr_range = coordinatorConfig.n - nr_min;
        coordinatorConfig.nr = nr_min + r.nextInt(nr_range);

      } else if (coordinatorConfig.quorumSelection == QuorumSelection.CONSISTENT) {
        /**********************************************************************
          [CONSISTENT] Nw and Nr set to N (fully consistent system)
        **********************************************************************/
        coordinatorConfig.nw = coordinatorConfig.n;
        coordinatorConfig.nr = coordinatorConfig.n;
      } else {
        /* ERROR: this shouldnt happen since converted to enum earlier */
        System.err.printf("[CoordinatorConfigurationManager] UNKNOWN QUORUM SELECTION OF '%s'.\n", coordinatorConfig.quorumSelection);
        System.exit(1);
      }
    }
  }




}
