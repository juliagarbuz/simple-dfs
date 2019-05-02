import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import java.util.*;
import java.util.stream.Stream;
import java.util.Random;

public class DFSNodeHandler implements DFSNode.Iface {

    private NodeInfo nodeInfo;
    private NodeInfo coordinatorInfo;

    private FileManager fileManager;

    // Only used if Node is COORDINATOR NODE:
    private DFSCoordinator dfsCoordinator;

    public DFSNodeHandler(NodeInfo info, CoordinatorConfiguration config) {
        // CONSTRUCTOR for [COORDINATOR NODE]
        nodeInfo = info;
        coordinatorInfo = info;
        System.out.printf(name() + " Constructing COORDINATOR NODE on '%s:%d'\n", info.ip, info.port);
        dfsCoordinator = new DFSCoordinator(nodeInfo, config);
        fileManager = new FileManager(nodeInfo);
    }

    public DFSNodeHandler(NodeInfo info, NodeInfo coordInfo) {
        // CONSTRUCTOR for [REGULAR NODE]
        System.out.printf("[DFSNode] Constructing REGULAR NODE on '%s:%d\n", info.ip, info.port);
        System.out.printf("[DFSNode] Coordinator Info: '%s:%d\n", coordInfo.ip, coordInfo.port);
        nodeInfo = info;
        coordinatorInfo = coordInfo;
        fileManager = new FileManager(nodeInfo);
    }

    public boolean isCoordinator() {
      return nodeInfo.isCoordinator;
    }

    public String name() {
      if (isCoordinator()) { return "[DFSNode]*"; }
      else { return "[DFSNode]"; }
    }

    @Override
    public Response joinDFS(NodeInfo newNode) throws TException {
      System.out.printf(name() + " Received JOIN(%s:%d) request.\n", newNode.ip, newNode.port);
      Response response;
      if (isCoordinator()) {
        response = dfsCoordinator.addNode(newNode);
      } else {
        response = new Response();
        response.acknowledgement = Acknowledgement.FAILURE;
        response.message = "Cannot 'JOIN' non-coordinator node.";
      }
      return response;
    }

    @Override
    public WriteResult write(String filename, String contents) {
      System.out.printf(name() + " received WRITE(%s) request.\n", filename);

      if (isCoordinator()) {

        WriteResult finalWriteResult = new WriteResult();
        finalWriteResult.response = new Response();
        finalWriteResult.response.acknowledgement = Acknowledgement.SUCCESS;
        finalWriteResult.response.message = "";

        if (!dfsCoordinator.isReady()) {
          System.out.printf(name() + " COORDINATOR not ready, rejecting WRITE(%s) request.\n", filename);
          finalWriteResult.response.acknowledgement = Acknowledgement.FAILURE;
          finalWriteResult.response.message = "Coordinator not ready. Check all nodes have joined and try again.";
          return finalWriteResult;
        }

        dfsCoordinator.acquireLockOnFile(filename);

        ArrayList<NodeInfo> writeQuorum = dfsCoordinator.buildWriteQuorum();
        int newVersion = dfsCoordinator.getNewWriteVersion(writeQuorum, filename);
        System.out.printf(name() + " WRITE(%s): Will write (VERSION %d) to %d replicas.\n", filename, newVersion, writeQuorum.size());

        for (NodeInfo writerNode : writeQuorum) {
          try {
            TTransport transport = new TSocket(writerNode.ip, writerNode.port);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            DFSNode.Client client = new DFSNode.Client(protocol);
            transport.open();
            System.out.printf(name() + " WRITE(%s): Forwarding PERFORM_WRITE() to (%s:%d).\n", filename, writerNode.ip, writerNode.port);
            WriteResult writersResult = client.performWrite(filename, contents, newVersion);
            transport.close();

            if (writersResult.response.acknowledgement == Acknowledgement.FAILURE) {
              System.out.printf(name() + " WRITE(%s): Failed to PERFORM_WRITE() on (%s:%d) because '%s'.\n", filename, writerNode.ip, writerNode.port, writersResult.response.message);
              finalWriteResult = writersResult; // want to send failed one back if it failed even once
              break;
            }
          } catch(TException e) {
            System.err.printf(name() + " WRITE(%s): Failed to connect to writer (%s:%d).\n", filename, writerNode.ip, writerNode.port);
            finalWriteResult.response.acknowledgement = Acknowledgement.FAILURE;
            finalWriteResult.response.message = "Failed to connect to writer";
          }
        }

        dfsCoordinator.releaseLockOnFile(filename);
        return finalWriteResult;

      } else {
        try {
          TTransport transport = new TSocket(coordinatorInfo.ip, coordinatorInfo.port);
          TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
          DFSNode.Client client = new DFSNode.Client(protocol);
          transport.open();
          System.out.printf(name() + " WRITE(%s): Forwarding to Coordinator Node.\n", filename);

          WriteResult writeResult = client.write(filename, contents);

          transport.close();
          return writeResult;
        } catch(TException e) {
          System.err.printf("[DFSNode] WRITE(%s): Failed to call WRITE on Coordinator Node.\n", filename);
          e.printStackTrace();
        }
      }
      return null;
    }

    @Override
    public ReadResult read(String filename) {
      System.out.printf(name() + " Received READ(%s) request.\n", filename);

      ReadResult readResult = new ReadResult();
      readResult.response = new Response();
      readResult.response.acknowledgement = Acknowledgement.FAILURE;
      readResult.response.message = "";
      readResult.contents = "";
      readResult.version = -1;

      if (isCoordinator()) {

        if (!dfsCoordinator.isReady()) {
          System.out.printf(name() + " COORDINATOR not ready, rejecting READ(%s) request.\n", filename);
          readResult.response.message = "Coordinator not ready. Check all nodes have joined and try again.";
          return readResult;
        }

        dfsCoordinator.acquireLockOnFile(filename);

        NodeInfo readerNode = dfsCoordinator.getReaderNode(filename);
        if (readerNode.port == -1) {
          System.out.printf(name() + " READ(%s): File not found.\n", filename);
          readResult.response.message = "File does not exist yet";
        } else {
          System.out.printf(name() + " READ(%s): Will read from %s:%d\n", filename, readerNode.ip, readerNode.port);
          try {
            TTransport transport = new TSocket(readerNode.ip, readerNode.port);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            DFSNode.Client client = new DFSNode.Client(protocol);
            transport.open();
            System.out.printf(name() + " READ(%s): Forwarding to reader (%s:%d).\n", filename, readerNode.ip, readerNode.port);
            readResult = client.performRead(filename);
            transport.close();
          } catch(TException e) {
            System.err.printf(name() + " READ(%s): Failed to connect to reader (%s:%d).\n", filename, readerNode.ip, readerNode.port);
            readResult.response.message = "Failed to connect to reader.";
          }
        }
        dfsCoordinator.releaseLockOnFile(filename);
      } else {
        try {
          TTransport transport = new TSocket(coordinatorInfo.ip, coordinatorInfo.port);
          TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
          DFSNode.Client client = new DFSNode.Client(protocol);
          transport.open();
          System.out.printf(name() + " READ(%s): Forwarding to Coordinator Node.\n", filename);
          readResult = client.read(filename);
          transport.close();
        } catch(TException e) {
          System.err.printf(name() + " READ(%s): Failed to connect to Coordinator Node.\n", filename);
          readResult.response.message = "Failed to forward read to coordinator node.";
        }
      }
      return readResult;
    }

    @Override
    public WriteResult performWrite(String filename, String contents, int version) {
      System.out.printf(name() + " Received PERFORM_WRITE(%s) request.\n", filename);
      return fileManager.performWrite(filename, contents, version);
    }

    @Override
    public ReadResult performRead(String filename) {
      System.out.printf(name() + " Received PERFORM_READ(%s) request.\n", filename);
      return fileManager.performRead(filename);
    }

    @Override
    public FileInfo getFileInfo(String filename) {
      System.out.printf(name() + " Received GET_FILE_INFO(%s) request.\n", filename);
      return fileManager.getFileInfo(filename);
    }

    @Override
    public Response update() {
      System.out.printf(name() + " Received UPDATE() request.\n");
      Response response = new Response();
      response.acknowledgement = Acknowledgement.SUCCESS;
      response.message = "";

      // for each file in file manager, call READ
      ArrayList<FileInfo> allFilesOnNode = fileManager.getAllFileInfos();
      for (FileInfo file : allFilesOnNode) {
        // Call read, fwd to coordinator, get updated info on file:
        ReadResult readResult = read(file.filename);
        if (readResult.response.acknowledgement == Acknowledgement.SUCCESS) {
          // Local write:
          WriteResult writeResult = fileManager.performWrite(file.filename, readResult.contents, readResult.version);
          if (writeResult.response.acknowledgement == Acknowledgement.FAILURE) {
            response.acknowledgement = Acknowledgement.FAILURE;
            response.message = "Failed to write to file '" + file.filename + "'";
            return response;
          }
          // else, continue to next file
        } else {
          response.acknowledgement = Acknowledgement.FAILURE;
          response.message = "Failed to read file '" + file.filename + "'";
          return response;
        }
      }
      System.out.printf("\n" + name() + " File versions post UPDATE().\n");
      fileManager.printAllFileVersions();
      return response;
    }

    @Override
    public NodeInfo getRandomNode() {
      System.out.printf(name() + " Received GET_RANDOM_NODE() request.\n");
      return dfsCoordinator.getRandomNode();
    }


    @Override
    public List<FileInfo> getFiles() {
      System.out.printf(name() + " Received GET_FILES() request.\n");
      return fileManager.getAllFileInfos();
    }

    @Override
    public List<FileInfo> getAllFileVersions() {
      System.out.printf(name() + " Received GET_ALL_FILE_VERSIONS() request.\n");
      if (isCoordinator()) {
        if (!dfsCoordinator.isReady()) {
          return null;
        }
        List<NodeInfo> readQuorum = dfsCoordinator.buildReadQuorum();
        List<FileInfo> allFiles = new ArrayList<FileInfo>();

        for (NodeInfo reader : readQuorum) {
          try {
            TTransport transport = new TSocket(reader.ip, reader.port);
            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
            DFSNode.Client client = new DFSNode.Client(protocol);

            transport.open();

            List<FileInfo> filesFromReader = client.getFiles();
            allFiles.addAll(filesFromReader);

            transport.close();

          } catch(TException e) {
            System.err.printf(name() + " GET_ALL_FILE_VERSIONS(): Failed to connect to reader node to get files.\n");
          }
        }
        allFiles = fileManager.filterToMostRecentFiles(allFiles);
        return allFiles;

      } else {
        try {
          TTransport transport = new TSocket(coordinatorInfo.ip, coordinatorInfo.port);
          TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
          DFSNode.Client client = new DFSNode.Client(protocol);
          transport.open();
          System.out.printf(name() + " GET_ALL_FILE_VERSIONS(): Forwarding to Coordinator Node.\n");
          List<FileInfo> files = client.getAllFileVersions();
          transport.close();
          return files;
        } catch(TException e) {
          System.err.printf(name() + " GET_ALL_FILE_VERSIONS(): Failed to connect to Coordinator Node.\n");
        }
      }
      return new ArrayList<FileInfo>();
    }


}
