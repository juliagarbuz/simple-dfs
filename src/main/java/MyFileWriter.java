
/**
* MyFileWriter abstracts out the complexities of writing to files and
* provides the option of writing the whole file or line by line, and
* overwriting or appending.
*
* @author  Julia Garbuz & Sean I. Geronimo Anderson
* @version 1.0
* @since   2019-03-03
*/

import java.io.*;
import java.util.ArrayList;

class MyFileWriter {

  private String filePath;
  private boolean fileOpen;
  private boolean appendMode;
  private File fileObject;
  private FileWriter fileWriter;
  private BufferedWriter bufferedWriter;

  private static final boolean DEFUALT_APPEND_MODE = false;

  /**
   * MyFileWriter constructor with default appendMode
   * @param inputFilePath Path to/name of file to write to
   * @return void
   */
  public MyFileWriter(String inputFilePath) {
    this.filePath = inputFilePath;
    this.fileOpen = false;
    this.appendMode = DEFUALT_APPEND_MODE;
  }

  /**
   * MyFileWriter constructor with appendMode
   * @param inputFilePath Path to/name of file to write to
   * @param inputAppendMode Whether or not appending to/overwriting file
   * @return void
   */
  public MyFileWriter(String inputFilePath, boolean inputAppendMode) {
    this.filePath = inputFilePath;
    this.fileOpen = false;
    this.appendMode = inputAppendMode;
  }

  /*****************
   * Basic GETTERS *
   *****************/

  public String getFilePath() { return this.filePath; }
  public boolean inAppendMode() { return this.appendMode; }
  public boolean isOpen() { return this.fileOpen; }

  /**
   * Opens file / intializes File and Writer objects
   * @return boolean of whether file opened/prepped successfully
   */
  public boolean openFile() {
    try {
      this.fileObject = new File(this.filePath);
      this.fileWriter = new FileWriter(this.fileObject, this.appendMode);
      this.bufferedWriter = new BufferedWriter(this.fileWriter);
      this.fileOpen = true;
    } catch(Exception e) {
      System.err.println("Encountered error '" + e + "' while opening '" +
       this.filePath + "' in MyFileWriter.");
    }
    return this.fileOpen;
  }

  /**
   * Closes bufferedWriter and fileWriter objects
   * @return boolean of whether file closed successfully
   */
  public boolean closeFile() {
    try {
      this.bufferedWriter.close();
      this.fileWriter.close();
      this.fileOpen = false;
    } catch (IOException e) {
      System.err.println("Couldn't close '" + this.filePath + "'");
    }
    return !this.fileOpen;
  }

  /**
   * Writes single string to file, but does not close (intended for append mode)
   * @param line single string user wants to add to file
   * @return boolean of whether wrote successfully
   */
  public boolean writeLine(String line) {
    // NOTE: does NOT close file, intended for append mode so user can call
    // this multiple times
    if (this.fileOpen || this.openFile()) {
      try {
        this.bufferedWriter.write(line + "\n");
        return true;
      } catch (IOException e) {
        System.err.println("Could not write line to '" + this.filePath + "'");
        return false;
      }
    } else {
      return false;
    }
  }

  /**
   * Writes single string to file and closes file
   * @param allLines all text user wants to write to file (as string)
   * @return boolean of whether wrote and closed successfully
   */
  public boolean writeLines(String allLines) {
    return this.writeLine(allLines.replaceAll("\n+$", "")) && this.closeFile();
  }

  /**
   * Writes each string of list on new line and then closes file
   * @param allLines all text user wants to write to file (as array of strings)
   * @return boolean of whether wrote and closed successfully
   */
  public boolean writeLines(ArrayList<String> allLines) {
    String allLinesAsString = String.join("\n", allLines);
    return this.writeLines(allLinesAsString);
  }

  // public static void main(String[] args) {
  //
  //   // TESTING FUNCTIONALITY:
  //
  //   String noAppendByLine = "noAppendByLine.txt";
  //   MyFileWriter noAppendByLineWriter = new MyFileWriter(noAppendByLine);
  //   System.out.println("FILEPATH:" + noAppendByLineWriter.getFilePath());
  //   System.out.println("IN APPEND MODE:" + noAppendByLineWriter.inAppendMode());
  //   System.out.println("OPEN FILE:" + noAppendByLineWriter.openFile());
  //   System.out.println("\nWRITE LINE 1:" + noAppendByLineWriter.writeLine("first"));
  //   System.out.println("\nWRITE LINE 2:" + noAppendByLineWriter.writeLine("second"));
  //   System.out.println("CLOSE FILE:" + noAppendByLineWriter.closeFile());
  //
  //   System.out.println("\n--------------------------------\n");
  //
  //   String demoFile = "demofile.txt";
  //   MyFileWriter myFileWriter = new MyFileWriter(demoFile);
  //   System.out.println("FILEPATH:" + myFileWriter.getFilePath());
  //   System.out.println("IN APPEND MODE:" + myFileWriter.inAppendMode());
  //   System.out.println("OPEN FILE:" + myFileWriter.openFile());
  //   System.out.println("\nWRITE LINE 1:" + myFileWriter.writeLine("Line 1"));
  //   System.out.println("\nWRITE LINE 2:" + myFileWriter.writeLine("Line 2"));
  //   System.out.println("CLOSE FILE:" + myFileWriter.closeFile());
  //   System.out.println("IS OPEN:" + myFileWriter.isOpen());
  //   System.out.println("\nWRITE LINE 3:" + myFileWriter.writeLine("Line 3"));
  //   System.out.println("CLOSE FILE:" + myFileWriter.closeFile());
  //   System.out.println("IS OPEN:" + myFileWriter.isOpen());
  //
  //   System.out.println("\n--------------------------------\n");
  //
  //   MyFileWriter myFileWriter_append = new MyFileWriter(demoFile, true);
  //   System.out.println("FILEPATH:" + myFileWriter_append.getFilePath());
  //   System.out.println("IN APPEND MODE:" + myFileWriter_append.inAppendMode());
  //   System.out.println("OPEN FILE:" + myFileWriter_append.openFile());
  //   System.out.println("\nWRITE LINE 4:" + myFileWriter_append.writeLine("Line 4"));
  //   System.out.println("\nWRITE LINE 5:" + myFileWriter_append.writeLine("Line 5"));
  //   System.out.println("CLOSE FILE:" + myFileWriter_append.closeFile());
  //   System.out.println("IS OPEN:" + myFileWriter_append.isOpen());
  //   System.out.println("\nWRITE LINE 6:" + myFileWriter_append.writeLine("Line 6"));
  //   System.out.println("CLOSE FILE:" + myFileWriter_append.closeFile());
  //   System.out.println("IS OPEN:" + myFileWriter_append.isOpen());
  //
  //   System.out.println("\n--------------------------------\n");
  //
  //   String writeWholeAsString = "writeWholeAsString.txt";
  //   MyFileWriter wholeFileAsString = new MyFileWriter(writeWholeAsString);
  //   System.out.println("FILEPATH:" + wholeFileAsString.getFilePath());
  //   System.out.println("IN APPEND MODE:" + wholeFileAsString.inAppendMode());
  //   System.out.println("OPEN FILE:" + wholeFileAsString.openFile());
  //   String wholeString = "Hello\nJulia\nGarbuz!";
  //   System.out.println("\nWRITE LINES:" + wholeFileAsString.writeLines(wholeString));
  //   System.out.println("IS OPEN:" + wholeFileAsString.isOpen());
  //   System.out.println("\nWRITE LINES AGAIN:" + wholeFileAsString.writeLines(wholeString));
  //   System.out.println("IS OPEN:" + wholeFileAsString.isOpen());
  //
  //   System.out.println("\n--------------------------------\n");
  //
  //   String writeWholeAsString_append = "writeWholeAsString_append.txt";
  //   MyFileWriter wholeFileAsStringAppend = new MyFileWriter(writeWholeAsString_append, true);
  //   System.out.println("FILEPATH:" + wholeFileAsStringAppend.getFilePath());
  //   System.out.println("IN APPEND MODE:" + wholeFileAsStringAppend.inAppendMode());
  //   System.out.println("OPEN FILE:" + wholeFileAsStringAppend.openFile());
  //   System.out.println("\nWRITE LINES:" + wholeFileAsStringAppend.writeLines(wholeString));
  //   System.out.println("IS OPEN:" + wholeFileAsStringAppend.isOpen());
  //   System.out.println("\nWRITE LINES AGAIN:" + wholeFileAsStringAppend.writeLines(wholeString));
  //   System.out.println("IS OPEN:" + wholeFileAsStringAppend.isOpen());
  //
  //   System.out.println("\n--------------------------------\n");
  //
  //   ArrayList<String> linesAsList = new ArrayList<>();
  //   linesAsList.add("first line");
  //   linesAsList.add("second line");
  //   linesAsList.add("third line");
  //
  //   ArrayList<String> linesAsList2 = new ArrayList<>();
  //   linesAsList2.add("fourth line");
  //   linesAsList2.add("fifth line");
  //   linesAsList2.add("sixth line");
  //
  //   MyFileWriter wholeFileFromArray = new MyFileWriter("wholeFileFromArray.txt");
  //   System.out.println("FILEPATH:" + wholeFileFromArray.getFilePath());
  //   System.out.println("IN APPEND MODE:" + wholeFileFromArray.inAppendMode());
  //   System.out.println("OPEN FILE:" + wholeFileFromArray.openFile());
  //   System.out.println("\nWRITE LINES:" + wholeFileFromArray.writeLines(linesAsList));
  //   System.out.println("IS OPEN:" + wholeFileFromArray.isOpen());
  //   System.out.println("\nWRITE LINES AGAIN:" + wholeFileFromArray.writeLines(linesAsList2));
  //   System.out.println("IS OPEN:" + wholeFileFromArray.isOpen());
  //
  // }

}
