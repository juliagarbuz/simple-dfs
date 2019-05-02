struct NodeInfo {
    1: required string ip;
    2: required i32 port;
    3: required bool isCoordinator;
}

enum QuorumSelection {
  USER_CONFIG = 0,
  RANDOM = 1,
  READ_HEAVY = 2,
  WRITE_HEAVY = 3,
  CONSISTENT = 4
}

enum Property {
  status = 0,
  ip = 1,
  port = 2,
  quorum_selection = 3,
  n = 4,
  nw = 5,
  nr = 6
  update_frequency
}

enum CoordinatorStatus {
  DOWN = 0,
  STARTING = 1,
  LIVE = 2
}

struct CoordinatorConfiguration {
  1: required QuorumSelection quorumSelection;
  2: required i32 n;
  3: required i32 nw;
  4: required i32 nr;
}

struct FileInfo {
  1: required NodeInfo sourceNode;
  2: required string filename;
  3: required bool exists;
  4: required i32 version; // set to -1 if file does not exist
}

enum Acknowledgement {
  SUCCESS = 0,
  FAILURE = 1
}

struct Response {
  1: required Acknowledgement acknowledgement;
  2: required string message; // Empty string if no msg needed
}

struct WriteResult {
  1: required Response response;
}

struct ReadResult {
  1: required Response response;
  2: required string contents;
  3: required i32 version;
}

service DFSNode {
  // (NODE --> COORDINATOR) Used to add nodes to DFS as they join
  Response joinDFS(1: NodeInfo nodeInfo);

  // (CLIENT --> NODE) and then (NODE --fwd--> COORDINATOR):
  WriteResult write(1: string filename, 2: string contents);
  ReadResult read(1: string filename);

  // (COORDINATOR --> NODE) to actually perform operation
  WriteResult performWrite(1: string filename, 2: string contents, 3: i32 version);
  ReadResult performRead(1: string filename);

  // Used by COORDINATOR to get versions from each node:
  FileInfo getFileInfo(1: string filename);

  // (CLIENT --> NODE --fwd--> COORDINATOR)
  // For displaying getting all file info to client on UI
  list<FileInfo> getAllFileVersions();

  // (COORDINATOR --> NODE)
  // For displaying getting all file info from node (used by getAllFileVersions())
  list<FileInfo> getFiles();

  // Periodically called on itself (NODE):
  Response update();

  // (CLIENT --> COORDINATOR) Called when client initialized to get contact
  // info of random node (if not provided via command line)
  NodeInfo getRandomNode();
}
