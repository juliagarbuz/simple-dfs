import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import java.util.*;
import java.io.*;
import java.nio.file.*;
import java.time.*;

import java.lang.Object;
import java.nio.charset.StandardCharsets;

public class Client {

  private static final String PATH_TO_ROOT = "../../../";
  private static final String PATH_TO_RESOURCES = "../resources/";
  private static final String DFS_PROPERTIES_FILE = "dfs.properties";

  private static final int DEFAULT_PORT = 7777;

  private static final int RETRY_ATTEMPTS = 5;

  private static NodeInfo entryNode;

  private static TTransport dfsNode_transport;
  private static TProtocol dfsNode_protocol;
  private static DFSNode.Client dfsNode_client;

  public static WriteResult makeWriteRequest(String filename, String contents, boolean contentsInFile) {
    if (contentsInFile) {
      MyFileReader reader = new MyFileReader(PATH_TO_ROOT + contents);
      if (reader.openFile()) {
        reader.readWholeFile();
        contents = reader.getFileContentsAsString();
        reader.closeFile();
      } else {
        WriteResult result = new WriteResult();
        result.response = new Response();
        result.response.acknowledgement = Acknowledgement.FAILURE;
        result.response.message = "Could not open file with contents to write.";
        return result;
      }
    }
    try {
      return dfsNode_client.write(filename, contents);
    } catch (TException e) {
      System.err.printf("[CLIENT] Failed to make write request.\n");
      return null;
    }
  }

  public static ReadResult makeReadRequest(String filename) {
    try {
      return dfsNode_client.read(filename);
    } catch (TException e) {
      System.err.printf("[CLIENT] Failed to make write request.\n");
      return null;
    }
  }

  public static List<FileInfo> makeGetFilesRequest() {
    try {
      return dfsNode_client.getAllFileVersions();
    } catch (TException e) {
      System.err.printf("[CLIENT] Failed to make makeGetFilesRequest() request.\n");
      return null;
    }
  }

  public static ArrayList<String> getFileOfRequestsContents(String filename) {
    MyFileReader reader = new MyFileReader(PATH_TO_ROOT+filename);
    if (reader.openFile()) {
      reader.readWholeFile();
      ArrayList<String> fileContents = reader.getFileContentsByLine();
      reader.closeFile();
      return fileContents;
    }
    return null;
  }

  public static void submitFileOfRequests(String filename, int depth) {
    ArrayList<String> requests = getFileOfRequestsContents(filename);
    if (requests == null) {
      List<String> errOutput = new ArrayList<String>();
      errOutput.add("  [RESPONSE]: FAILURE (File '" + filename + "' not found)");
      printRequestOutput(errOutput, depth);
    } else {
      for (String request : requests) {
        // Get sub-request output
        List<String> subRequestOutput = processRequest(request, depth+1);
        printRequestOutput(subRequestOutput, depth+1);
      }
    }
  }

  public static List<String> formatFileVersionList(List<FileInfo> files) {
    String doubleLine = "  ============================================================";
    String singleLine = "  ------------------------------------------------------------";
    List<String> fileList = new ArrayList<String>();

    fileList.add(doubleLine);
    fileList.add("  [FILES IN DFS " + files.size() + "]:");
    fileList.add(singleLine);

    for (FileInfo file : files) {
        fileList.add("\t  " + "[" + file.filename + "]:\tVERSION " +
          Integer.toString(file.version));
    }
    fileList.add(doubleLine);
    return fileList;
  }

  public static List<String> formatFileContents(ReadResult readResult) {
    List<String> contentsByLines = new ArrayList<String>(Arrays.asList(readResult.contents.split("\n")));

    String doubleLine = "  ============================================================";
    String singleLine = "  ------------------------------------------------------------";
    List<String> fileInfo = new ArrayList<String>();
    fileInfo.add(doubleLine);
    fileInfo.add("  [FILE VERSION: " + readResult.version + "]:");
    fileInfo.add(singleLine);

    for (String line : contentsByLines) {
      fileInfo.add("  " + line);
    }
    fileInfo.add(doubleLine);
    return fileInfo;
  }

  public static void printRequestOutput(List<String> output, int depth) {
    String indent = "\t";
    for (int i = 0; i < depth; i++) {
      indent += "\t";
    }
    System.out.println();
    for (String line : output) {
      System.out.println(indent + line);
    }
  }

  public static long getTimeElapsed(Instant start){
    Instant end = Instant.now();
    long milliElapsed = Duration.between(start, end).toMillis();
    return milliElapsed;
  }

  public static List<String> processRequest(String request) {
    return processRequest(request, 0);
  }

  public static List<String> processRequest(String request, int depth) {

    Instant start = Instant.now();

    ArrayList<String> split_request = new ArrayList<String>(Arrays.asList(request.split(",")));
    List<String> output = new ArrayList<String>();

    if (split_request.get(0).toLowerCase().contains("write")) {
      if (split_request.size() != 3) {
        output.add("  [WRITE requires 2 arguments: filename, contents] Please try again.");
      } else {
        String filename = split_request.get(1).trim();
        String contents = split_request.get(2).trim();
        WriteResult writeResult;
        if (contents.length() > 2 && contents.substring(0,3).contains("-f")) {
          String fileWithContents = contents.substring(2, contents.length()).trim();
          output.add("• [REQUEST]: WRITE CONTENTS OF '" + fileWithContents + "' TO FILE '" + filename + "'");
          writeResult = makeWriteRequest(filename, fileWithContents, true);
        } else {
          output.add("• [REQUEST]: WRITE '" + contents + "' TO FILE '" + filename + "'");
          writeResult = makeWriteRequest(filename, contents, false);
        }
        if (writeResult.response.acknowledgement == Acknowledgement.SUCCESS) {
          output.add("  [Completed request in " + Long.toString(getTimeElapsed(start)) + " milliseconds]");
          output.add("  [RESPONSE]: " + writeResult.response.acknowledgement.name());
          return output;
        } else {
          output.add("  [Completed request in " + Long.toString(getTimeElapsed(start)) + " milliseconds]");
          output.add("  [RESPONSE]: " + writeResult.response.acknowledgement.name() + " (" + writeResult.response.message + ")");
          return output;
        }
      }
    } else if (split_request.get(0).toLowerCase().contains("read")) {
      if (split_request.size() != 2) {
        output.add("  [READ requires 1 argument] Please try again.");
      } else {
        String filename = split_request.get(1).trim();
        output.add("• [REQUEST]: READ CONTENTS OF FILE '" + filename + "'");
        ReadResult readResult = makeReadRequest(filename);
        if (readResult == null || readResult.response.acknowledgement == Acknowledgement.FAILURE) {
          output.add("  [Completed request in " + Long.toString(getTimeElapsed(start)) + " milliseconds]");
          output.add("  [RESPONSE]: " + readResult.response.acknowledgement.name() + " (" + readResult.response.message + ")");
          return output;
        } else {
          output.add("  [Completed request in " + Long.toString(getTimeElapsed(start)) + " milliseconds]");
          output.add("  [RESPONSE]: " + readResult.response.acknowledgement.name());
          output.addAll(formatFileContents(readResult));
          return output;
        }
      }
    } else if (split_request.contains("ls")) {
      output.add("• [REQUEST]: GET LIST OF FILES");
      List<FileInfo> files = makeGetFilesRequest();
      if (files == null) {
        output.add("  [RESPONSE]: FAILURE (Coordinator not ready. Check all nodes have joined and try again.)");
      } else {
        output.add("  [Completed request in " + Long.toString(getTimeElapsed(start)) + " milliseconds]");
        output.addAll(formatFileVersionList(files));
      }

      return output;
    } else if (split_request.get(0).toLowerCase().contains("submit")) {
      String filename = split_request.get(1).trim();
      List<String> before = new ArrayList();
      before.add("• [REQUEST]: SUBMIT REQUESTS IN FILE '" + filename + "'");
      printRequestOutput(before, depth);
      submitFileOfRequests(filename, depth);
      List<String> after = new ArrayList();
      after.add("  [Completed request in " + Long.toString(getTimeElapsed(start)) + " milliseconds]");
      printRequestOutput(after, depth);
      return output;
    } else {
      output.add("  [UNKNOWN COMMAND] Please try again.");
    }
    return output;
  }

  public static void runCLI() {
    Scanner scanner = new Scanner(System.in);  // Create a Scanner object

    System.out.println(
      "============================================================================================\n" +
      "DFS CLI INSTRUCTIONS:\n" +
      "--------------------------------------------------------------------------------------------\n" +
      "(Please make sure you comma-separate the commands and arguments)\n" +

      "\n\tWRITE, [filename], [contents to write to file]\n" +
      "\t\t> write, file1.txt, this will be the contents of the file\n" +
      "\t\t> write, file1.txt, -f pathFromRoot/fileContainingContents.txt\n" +

      "\n\tREAD, [filename]\n" +
      "\t\t> read, file1.txt\n" +

      "\n\tLIST FILES AND VERSIONS:\n" +
      "\t\t> ls\n" +

      "\n\tSUBMIT FILE WITH MANY REQUESTS:\n" +
      "\t(each request on new line and must be formatted as shown above (comma-separated))\n" +
      "\t\t> submit, pathFromRoot/fileWithRequests.txt\n" +

      "============================================================================================\n"
      );

    System.out.print("\n> ");
    String input = scanner.nextLine();

    while (!input.equals("")) {

      List<String> response = processRequest(input);
      if (response.size() != 0) {
        printRequestOutput(response, 0);
      }

      System.out.print("\n> ");
      input = scanner.nextLine();
    }

  }

  public static NodeInfo readCoordinatorConfigsFromFile() {
    NodeInfo coordinatorInfo = new NodeInfo();

    String userPropertiesFilePath = PATH_TO_RESOURCES + DFS_PROPERTIES_FILE;
    Properties userDefinedProperties = new Properties();
    try {
        userDefinedProperties.load(new FileInputStream(userPropertiesFilePath));
    } catch (Exception e) {
      e.printStackTrace();
    }

    CoordinatorStatus currentCoordinatorStatus =
      CoordinatorStatus.valueOf(userDefinedProperties.getProperty(Property.status.name()));
    if (currentCoordinatorStatus != CoordinatorStatus.LIVE) {
      System.err.printf("[Client] Coordinator node is not yet live. Please make sure you started one and try again.\n");
      System.exit(1);
    } else {
        coordinatorInfo.ip = userDefinedProperties.getProperty(Property.ip.name());
        coordinatorInfo.port = Integer.valueOf(userDefinedProperties.getProperty(Property.port.name()));
        coordinatorInfo.isCoordinator = true;
    }
    return coordinatorInfo;
  }

  public static NodeInfo getRandomNode(NodeInfo coordinatorInfo) {
    System.out.printf("[Client] Contacting coordinator to get random DFS Node.\n");

    try {
      TTransport transport = new TSocket(coordinatorInfo.ip, coordinatorInfo.port);
      TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
      DFSNode.Client client = new DFSNode.Client(protocol);
      transport.open();
      System.out.printf("[Client] Connected to Coordinator Node '" + coordinatorInfo.ip + ":" + Integer.toString(coordinatorInfo.port) + "'\n");
      NodeInfo randomNode =  client.getRandomNode();
      transport.close();
      return randomNode;

    } catch(TException e) {
      System.err.printf("[Client] Failed to connect to Coordinator Node '" + coordinatorInfo.ip + ":" + Integer.toString(coordinatorInfo.port) + "'\n");
      System.exit(1);
    }
    return null;
  }

  public static void main(String [] args) {

    if (args.length == 0) {
      // [0] Connect to coordinator to get node info
      NodeInfo coordinatorInfo = readCoordinatorConfigsFromFile();
      entryNode = getRandomNode(coordinatorInfo);

    } else if (args.length == 1) {
      // [1: node_ip:node_port]
      entryNode = new NodeInfo();
      String[] nodeIpAndPort = args[1].split(":");
      entryNode.ip = nodeIpAndPort[0];
      entryNode.port = Integer.valueOf(nodeIpAndPort[1]);

    } else {
      System.err.printf("[Client] UNKNOWN NUMBER OF ARGUMENTS (%d). EXPECTING 1 OR 2.\n", args.length);
      System.exit(1);
    }

    System.out.printf("[Client] Set to connect with Random DFS Node at '" + entryNode.ip + ":" + Integer.toString(entryNode.port) + "'\n");

    try {
      dfsNode_transport = new TSocket(entryNode.ip, entryNode.port);
      dfsNode_protocol = new TBinaryProtocol(new TFramedTransport(dfsNode_transport));
      dfsNode_client = new DFSNode.Client(dfsNode_protocol);

      /***********************************************************************************/
      /*************************** OPEN CONNECTION TO DFS NODE ***************************/
      dfsNode_transport.open();
      System.out.printf("[Client] Connected to DFS Node '" + entryNode.ip + ":" + Integer.toString(entryNode.port) + "'\n");
      /***********************************************************************************/

      /***********************************************************************************/
      /****************** COLLECT INPUT AND SUBMIT REQUESTS TO DFS NODE ******************/
      runCLI();
      /***********************************************************************************/

      /***********************************************************************************/
      /************************** CLOSE CONNECTION TO DFS NODE ***************************/
      dfsNode_transport.close();
      /***********************************************************************************/

    } catch(TException e) {
      System.err.printf("[Client] Failed to connect to DFS Node '" + entryNode.ip + ":" + Integer.toString(entryNode.port) + "'\n");
      System.exit(1);
    }

  }
}
