import java.util.*;
import java.util.concurrent.Semaphore;

class FileManager {

  private static final String PATH_TO_ROOT = "../../../";
  private static final String DATA_DIRECTORY = PATH_TO_ROOT + "data/";

  private NodeInfo nodeInfo;
  private String pathToNodesDataDir;

  private HashMap<String, Integer> fileVersions = new HashMap<String, Integer>();
  private HashMap<String, Semaphore> fileLocks = new HashMap<String, Semaphore>();

  public FileManager(NodeInfo info) {
    nodeInfo = info;
    pathToNodesDataDir = DATA_DIRECTORY + nodeInfo.ip + ":" +
      Integer.toString(nodeInfo.port) + "/";
  }

  /************************************************************************************************
   * printAllFileVersions(): Used for debug printing
   ************************************************************************************************/
  public void printAllFileVersions() {
    String line = "============================================================";
    String line2 = "------------------------------------------------------------";
    String allInfo =  line + "\n" +
                      "[LIST OF FILES ON: " + nodeInfo.ip + ":" +
                        Integer.toString(nodeInfo.port) + "]\n" +
                      line2 + "\n";
    for (String filename : fileVersions.keySet()) {
      allInfo += "\t" + "[" + filename + "]:\tVERSION " +
        Integer.toString(fileVersions.get(filename)) + "\n";
    }
    allInfo += line + "\n";
    System.out.println(allInfo);
  }

  /************************************************************************************************
   * getFileInfo(): Returns FileInfo object (used to get version)
   ************************************************************************************************/
  public FileInfo getFileInfo(String filename) {
    FileInfo fileInfo = new FileInfo();
    fileInfo.sourceNode = nodeInfo;
    fileInfo.filename = filename;
    fileInfo.exists = fileVersions.containsKey(filename);
    if (fileInfo.exists) {
      fileInfo.version = fileVersions.get(filename);
    } else { fileInfo.version = -1; }
    return fileInfo;
  }

  /************************************************************************************************
   * getAllFileInfos(): Used to then write print/status method for client
   ************************************************************************************************/
  public ArrayList<FileInfo> getAllFileInfos() {
    ArrayList<FileInfo> fileInfos = new ArrayList<FileInfo>(fileVersions.size());
    for (String filename : fileVersions.keySet()) {
      fileInfos.add(getFileInfo(filename));
    }
    return fileInfos;
  }

  /************************************************************************************************
   * performWrite(): physically writes contents to file and updates version
   ************************************************************************************************/
  public WriteResult performWrite(String filename, String contents, int version) {
    WriteResult writeResult = new WriteResult();
    writeResult.response = new Response();

    // If file doesn't exist yet, add to file manager:
    if (!fileVersions.containsKey(filename)) {
      fileVersions.put(filename, -1);
      fileLocks.put(filename, new Semaphore(1));
    }

    // Reject request if somehow got request for earlier version than already saved
    if (fileVersions.get(filename) > version) {
      writeResult.response.acknowledgement = Acknowledgement.FAILURE;
      writeResult.response.message = "Rejected write. Local version (" +
        Integer.toString(fileVersions.get(filename)) +
        ") greater than write request version (" +
        Integer.toString(version) + ").";
      return writeResult;
    }

    /******************* START CRITICAL SECTION (MUST LOCK INIDIVIDUAL FILE): ******************/
    Semaphore fileSemaphore = fileLocks.get(filename);
    try {
      fileSemaphore.acquire();
    } catch (InterruptedException ie) {
      System.err.printf("[FileManager] On '" + nodeInfo.ip + ":" +
        Integer.toString(nodeInfo.port) + "' unable to acquire lock for '" +
        filename + "'.\n");
    }

    MyFileWriter writer = new MyFileWriter(pathToNodesDataDir + filename);
    if (writer.openFile() && writer.writeLines(contents)) {
      fileVersions.put(filename, version);
      writeResult.response.acknowledgement = Acknowledgement.SUCCESS;
      writeResult.response.message = "";
    } else {
      writeResult.response.acknowledgement = Acknowledgement.FAILURE;
      writeResult.response.message = "Could not perform write to '" + filename +
        "' on node '" + nodeInfo.ip + ":" + Integer.toString(nodeInfo.port) +
        "' because unable to open file.";

    }
    fileSemaphore.release();
    /*********************************** END CRITICAL SECTION **********************************/
    return writeResult;
  }

  /************************************************************************************************
   * performRead(): physically read from file
   ************************************************************************************************/
  public ReadResult performRead(String filename) {
    ReadResult readResult = new ReadResult();
    readResult.response = new Response();

    // If file doesn't exist yet, immediately reject:
    if (!fileVersions.containsKey(filename)) {
      readResult.response.acknowledgement = Acknowledgement.FAILURE;
      readResult.response.message = "Could not perform read '" + filename +
        "' on node '" + nodeInfo.ip + ":" + Integer.toString(nodeInfo.port) +
        "' because file not found.";
      readResult.contents = "";
      readResult.version = -1;
      return readResult;
    } else {
      /******************* START CRITICAL SECTION (MUST LOCK INIDIVIDUAL FILE): ******************/
      Semaphore fileSemaphore = fileLocks.get(filename);
      try {
        fileSemaphore.acquire();
      } catch (InterruptedException ie) {
        System.err.printf("[FileManager] On '" + nodeInfo.ip + ":" +
          Integer.toString(nodeInfo.port) + "' unable to acquire lock for '" +
          filename + "'.\n");
        readResult.response.acknowledgement = Acknowledgement.FAILURE;
        readResult.response.message = "FileManager unable to acquire lock for file.";
        readResult.contents = "";
        readResult.version = -1;
      }

      MyFileReader reader = new MyFileReader(pathToNodesDataDir + filename);
      if (reader.openFile()) {
        reader.readWholeFile();
        readResult.contents = reader.getFileContentsAsString();
        readResult.response.acknowledgement = Acknowledgement.SUCCESS;
        readResult.response.message = "";
        readResult.version = fileVersions.get(filename);
      } else {
        readResult.contents = "";
        readResult.response.acknowledgement = Acknowledgement.FAILURE;
        readResult.response.message = "Could not perform read '" + filename +
          "' on node '" + nodeInfo.ip + ":" + Integer.toString(nodeInfo.port) +
          "' because unable to open file.";
        readResult.version = -1;
      }
      fileSemaphore.release();
      /*********************************** END CRITICAL SECTION **********************************/
      return readResult;
    }
  }

  /************************************************************************************************
   * perforfilterToMostRecentFilesmRead():
   ************************************************************************************************/
  public List<FileInfo> filterToMostRecentFiles(List<FileInfo> allFiles) {
    HashMap<String, List<FileInfo>> fileInfosByFileName = new HashMap<String, List<FileInfo>>();

    // Sort into HashMap by filename
    for (FileInfo fileInfo : allFiles) {
        if (!fileInfosByFileName.containsKey(fileInfo.filename)) {
          fileInfosByFileName.put(fileInfo.filename, new ArrayList<FileInfo>());
        }
        List<FileInfo> fileInfosForFile = fileInfosByFileName.get(fileInfo.filename);
        fileInfosForFile.add(fileInfo);
    }

    // For each filename, get fileInfos with most recent version and append to final list
    List<FileInfo> mostRecentFileVersions = new ArrayList<FileInfo>(fileInfosByFileName.size());

    for (String filename : fileInfosByFileName.keySet()) {

      List<FileInfo> fileInfos = fileInfosByFileName.get(filename);

      FileInfo fileWithVersion = new FileInfo();
      int mostRecentVersion = -1;

      for (FileInfo fileInfo : fileInfos) {
        if (fileInfo.version > mostRecentVersion) {
          mostRecentVersion = fileInfo.version;
          fileWithVersion = fileInfo;
        }
      }
      // ADD TO FINAL LIST
      mostRecentFileVersions.add(fileWithVersion);
    }

    return mostRecentFileVersions;
  }

}
