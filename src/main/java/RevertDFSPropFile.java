import java.util.*;

class RevertDFSPropFile {

  private static final String PATH_TO_RESOURCES = "../resources/";
  private static final String DFS_PROPERTIES_FILE = "dfs.properties";
  private static final String DFS_DEFAULTS_FILE = "default.properties";

  public static void cleanCoordinatorPropFile() {
    String userPropertiesFilePath = PATH_TO_RESOURCES + DFS_PROPERTIES_FILE;
    MyFileReader reader = new MyFileReader(userPropertiesFilePath);
    if (reader.openFile()) {
      reader.readWholeFile();
      ArrayList<String> fileContents = reader.getFileContentsByLine();
      reader.closeFile();
      int i = 0;
      int ip_idx = -1;
      int status_idx = -1;
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

  public static void main(String[] args) {
    cleanCoordinatorPropFile();
  }
}
