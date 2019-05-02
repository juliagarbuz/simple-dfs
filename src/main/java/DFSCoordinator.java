import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import java.util.*;
import java.util.concurrent.Semaphore;

class DFSCoordinator {

  // Configs, info, etc
  private NodeInfo nodeInfo;
  private CoordinatorConfiguration coordinatorConfig;

  // Ready flag
  private Semaphore readyLock = new Semaphore(1);
  private boolean ready;

  // Nodes
  private Semaphore nodesListLock = new Semaphore(1);
  private ArrayList<NodeInfo> nodes;

  // Open/queued files
  private Semaphore queuedFileListLock = new Semaphore(1);
  private HashMap<String, Semaphore> queuedFiles = new HashMap<String, Semaphore>();

  public DFSCoordinator(NodeInfo info, CoordinatorConfiguration config) {
    ready = false;
    nodeInfo = info;
    coordinatorConfig = config;

    try {
      nodesListLock.acquire();
      nodes = new ArrayList<>(coordinatorConfig.n);
      nodes.add(nodeInfo);
      printNodesList();
      nodesListLock.release();
    } catch (InterruptedException ie) {
      System.err.printf("[DFSCoordinator] InterruptedException while acquiring lock on nodes list.\n");
    }
  }

  public boolean isReady() {
    try {
      readyLock.acquire();
    } catch (InterruptedException ie) {
      System.err.printf("[DFSCoordinator] InterruptedException while acquiring lock on 'ready' flag.\n");
    }
    boolean result = ready;
    readyLock.release();
    return result;
  }

  /************************************************************************************************
   *************************************** FILE LOCK METHODS **************************************
   ************************************************************************************************/

  public void acquireLockOnFile(String filename) {
    try {
      queuedFileListLock.acquire();
      if (!queuedFiles.containsKey(filename)) {
        queuedFiles.put(filename, new Semaphore(1));
      }
      Semaphore fileLock = queuedFiles.get(filename);
      fileLock.acquire();
    } catch (InterruptedException ie) {
      System.err.printf("[DFSCoordinator] InterruptedException while acquiring lock on file '%s'.\n", filename);
    }
  }

  public void releaseLockOnFile(String filename) {
    Semaphore fileLock = queuedFiles.get(filename);
    fileLock.release();
    queuedFileListLock.release();
  }

  /************************************************************************************************
   *************************************** ALL PRINT METHODS **************************************
   ************************************************************************************************/

  public String fileSourceAndVersionToString(FileInfo f) {
    return "[" + f.sourceNode.ip + ":" + Integer.toString(f.sourceNode.port) + "]:\t" + (f.exists ? Integer.toString(f.version) : "--") + "\n";
  }

  public void printFileVersionsByNode(ArrayList<FileInfo> fileInfos) {
    String line = "============================================================";
    String line2 = "------------------------------------------------------------\n";
    String listAsString = "\n" + line + "\n[QUORUM'S INFO ON FILE '" + fileInfos.get(0).filename + "']: \n" + line2;
    for (FileInfo f : fileInfos) {
      listAsString += "\t" + fileSourceAndVersionToString(f);
    }
    System.out.println(listAsString + line + "\n");
  }

  public String fileNameAndVersionToString(FileInfo f) {
    return "[" + f.filename + "]:\t" + (f.exists ? Integer.toString(f.version) : "--") + "\n";
  }

  public void printAllFileVersions(ArrayList<FileInfo> fileInfos) {
    String line = "============================================================";
    String line2 = "------------------------------------------------------------\n";
    String listAsString = "\n" + line + "\n[MOST RECENT VERISONS OF ALL FILES]: \n" + line2;
    for (FileInfo f : fileInfos) {
      listAsString += "\t" + fileNameAndVersionToString(f);
    }
    System.out.println(listAsString + line + "\n");
  }

  public String nodeInfoToString(NodeInfo n) {
    return "[" + n.ip + ":" + Integer.toString(n.port) + "]" + (n.isCoordinator ? "\t*COORDINATOR\t" : "") + "\n";
  }

  public void printNodesList() {
    String line = "============================================================";
    String line2 = "------------------------------------------------------------\n";
    String listAsString = "\n" + line + "\n[LIST OF NODES IN DFS (" + Integer.toString(nodes.size()) + " of " + Integer.toString(coordinatorConfig.n) + ")]: \n" + line2;
    for (NodeInfo n : nodes) {
      listAsString += "\t" + nodeInfoToString(n);
    }
    System.out.println(listAsString + line + "\n");
  }

  /************************************************************************************************
   addNode():
   ************************************************************************************************/
  public Response addNode(NodeInfo newNode) {
    System.out.printf("[DFSCoordinator] Received JOIN request from '%s:%d' --> ",
            newNode.ip,
            newNode.port);
    Response response = new Response();

    try {
      readyLock.acquire();
    } catch (InterruptedException ie) {
      System.err.printf("[DFSCoordinator] InterruptedException while acquiring lock on 'ready' flag.\n");
    }

    if (ready) {
      response.acknowledgement = Acknowledgement.FAILURE;
      response.message = "DFS is at full capacity (" + Integer.toString(coordinatorConfig.n) + " nodes)";
      System.out.printf("REJECTED\n");
    } else {

      try {
        nodesListLock.acquire();
        nodes.add(newNode);
        System.out.printf("ACCEPTED\n");
        printNodesList();
        nodesListLock.release();
      } catch (InterruptedException ie) {
        System.err.printf("[DFSCoordinator] InterruptedException while acquiring lock on nodes list.\n");
      }

      response.acknowledgement = Acknowledgement.SUCCESS;
      response.message = "";
      if (!ready && nodes.size() == coordinatorConfig.n) {
        ready = true;
      }
    }
    readyLock.release();
    return response;
  }

  /************************************************************************************************
   getRandomNode():
   ************************************************************************************************/
  public NodeInfo getRandomNode() {
    Random r = new Random();
    try {
      nodesListLock.acquire();
      NodeInfo randomNode = nodes.get(r.nextInt(nodes.size()));
      nodesListLock.release();
      return randomNode;
    } catch (InterruptedException ie) {
      System.err.printf("[DFSCoordinator] InterruptedException while acquiring lock on nodes list.\n");
    }
    return null;
  }

  /************************************************************************************************
   printQuorum():
   ************************************************************************************************/
  public void printQuorum(ArrayList<NodeInfo> quorum, boolean isWriteQuorum) {
    String line = "============================================================";
    String line2 = "------------------------------------------------------------\n";
    String listAsString = "\n" + line + "\n[LIST OF (" + quorum.size() + ") NODES IN " +
    (isWriteQuorum ? "WRITE" : "READ") + " QUORUM]: \n" + line2;
    for (NodeInfo n : quorum) {
      listAsString += "\t" + nodeInfoToString(n);
    }
    System.out.println(listAsString + line + "\n");
  }

  /************************************************************************************************
   buildQuorum():
   ************************************************************************************************/
  public ArrayList<NodeInfo> buildQuorum(int num) {
    System.out.printf("[DFSCoordinator] Building quorum of size %d\n", num);
    ArrayList<NodeInfo> quorum = new ArrayList<NodeInfo>(num);

    HashSet<Integer> nodeIndices = new HashSet<Integer>(num);
    Random r = new Random();
    while (num != 0) {
      int new_idx = r.nextInt(nodes.size());
      if (!nodeIndices.contains(new_idx)) {
        nodeIndices.add(new_idx);
        num--;
      }
    }
    try {
      nodesListLock.acquire();
      for (Integer i : nodeIndices) {
        quorum.add(nodes.get(i));
      }
      nodesListLock.release();
    } catch (InterruptedException ie) {
      System.err.printf("[DFSCoordinator] InterruptedException while acquiring lock on nodes list.\n");
    }
    return quorum;
  }

  /************************************************************************************************
   buildReadQuorum():
   ************************************************************************************************/
  public ArrayList<NodeInfo> buildReadQuorum() {
    System.out.printf("[DFSCoordinator] Building read quorum...\n");
    ArrayList<NodeInfo> readQuorum = buildQuorum(coordinatorConfig.nr);
    printQuorum(readQuorum, false);
    return readQuorum;
  }

  /************************************************************************************************
   buildWriteQuorum():
   ************************************************************************************************/
  public ArrayList<NodeInfo> buildWriteQuorum() {
    System.out.printf("[DFSCoordinator] Building write quorum...\n");
    ArrayList<NodeInfo> writeQuorum = buildQuorum(coordinatorConfig.nw);
    printQuorum(writeQuorum, true);
    return writeQuorum;
  }

  /************************************************************************************************
   getFileInfos():
   ************************************************************************************************/
  public ArrayList<FileInfo> getFileInfos(ArrayList<NodeInfo> quorum, String filename) {
    ArrayList<FileInfo> fileInfos = new ArrayList<FileInfo>(quorum.size());
    for (NodeInfo n : quorum) {
      try {
        TTransport transport = new TSocket(n.ip, n.port);
        TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
        DFSNode.Client client = new DFSNode.Client(protocol);
        transport.open();
        fileInfos.add(client.getFileInfo(filename));
        transport.close();

      } catch(TException e) {
        System.err.printf("[DFSCoordinator] getFileInfos(): Failed to connect to %s:%d.\n", n.ip, n.port);
      }
    }
    System.out.printf("[DFSCoordinator] getFileInfos(): Collected all file info\n");
    printFileVersionsByNode(fileInfos);
    return fileInfos;
  }

  /************************************************************************************************
   getMostRecentFileVersion():
   ************************************************************************************************/
  public int getMostRecentFileVersion(ArrayList<FileInfo> fileInfos) {
    int maxVersion = -1;
    for (FileInfo fileInfo : fileInfos) {
      if (fileInfo.exists && fileInfo.version > maxVersion) {
        maxVersion = fileInfo.version;
      }
    }
    System.out.printf("[DFSCoordinator] Most recent version of '%s' found: %d\n", fileInfos.get(0).filename, maxVersion);
    return maxVersion;
  }

  /************************************************************************************************
   getNodeWithMostRecentVersion():
   ************************************************************************************************/
  public NodeInfo getNodeWithMostRecentVersion(ArrayList<FileInfo> fileInfos) {
    int mostRecentVersion = getMostRecentFileVersion(fileInfos);

    if (mostRecentVersion == -1) {
      System.out.printf("[DFSCoordinator] File '%s' not found.\n", fileInfos.get(0).filename);
    } else {
      for (FileInfo fileInfo : fileInfos) {
        if (fileInfo.exists && fileInfo.version == mostRecentVersion) {
          return fileInfo.sourceNode;
        }
      }
    }

    NodeInfo fileDoesntExist_dummyNode = new NodeInfo();
    fileDoesntExist_dummyNode.ip = "";
    fileDoesntExist_dummyNode.port = -1;
    fileDoesntExist_dummyNode.isCoordinator = false;
    return fileDoesntExist_dummyNode;
  }

  /************************************************************************************************
   getNewWriteVersion():
   ************************************************************************************************/
  public int getNewWriteVersion(ArrayList<NodeInfo> writeQuorum, String filename) {
    ArrayList<FileInfo> fileInfos = getFileInfos(writeQuorum, filename);
    int mostRecentVersion = getMostRecentFileVersion(fileInfos);
    if (mostRecentVersion == -1) { return 1; }
    else { return mostRecentVersion + 1; }
  }

  /************************************************************************************************
   getReaderNode():
   ************************************************************************************************/
  public NodeInfo getReaderNode(String filename) {
    ArrayList<NodeInfo> readQuorum = buildReadQuorum();
    ArrayList<FileInfo> fileInfos = getFileInfos(readQuorum, filename);
    System.out.printf("[DFSCoordinator] Getting node with most recent version for read.\n");
    return getNodeWithMostRecentVersion(fileInfos);
  }

}
