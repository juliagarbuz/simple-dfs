/**
* MyFileReader abstracts out the complexities of file reading/scanning and
* provides the option of reading the whole file or line by line.
*
* @author  Julia Garbuz & Sean I. Geronimo Anderson
* @version 1.0
* @since   2019-02-28
*/

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

class MyFileReader {

  private String filePath;
  private File fileObject;
  private Scanner fileScanner;
  private ArrayList<String> fileContentsByLine;
  private int currentLineNumber;
  private Boolean readWholeFile;

  /**
   * This is the only MyFileReader constructor
   * @param filePath Path to/name of file to read
   * @return void
   */
  public MyFileReader(String inputFilePath) {
    this.filePath = inputFilePath;
    this.fileContentsByLine = new ArrayList<String>();
    this.currentLineNumber = -1;
    this.readWholeFile = false;
  }

  /*****************
   * Basic GETTERS *
   *****************/

  public String getFilePath() {
    return this.filePath;
  }

  public File getFileObject() {
    return this.fileObject;
  }

  public ArrayList<String> getFileContentsByLine() {
    return this.fileContentsByLine;
  }

  public String getFileContentsAsString() {
    String contents = "";
    for (String line : this.fileContentsByLine) {
      contents += line + "\n";
    }
    return contents;
  }

  public int getCurrentLineNumber() {
    return this.currentLineNumber;
  }

  public String getCurrentLine() {
    return this.fileContentsByLine.get(this.currentLineNumber);
  }

  /**
   * Opens file / intializes File and Scanner objects
   * @return boolean of whether file found/opened successfully
   */
  public boolean openFile() {
    try {
      this.fileObject = new File(this.filePath);
      this.fileScanner = new Scanner(this.fileObject);
      return true;
    } catch(FileNotFoundException e) {
      // System.err.println("FILE '" + this.filePath + "' NOT FOUND");
      return false;
    }
  }

  /**
   * Closes fileScanner objects
   * @return boolean of whether file closed successfully
   */
  public boolean closeFile() {
    try {
      this.fileScanner.close();
      return true;
    } catch (Exception e) {
      System.err.println("Couldn't close '" + this.filePath + "'");
    }
    return false;
  }

  /**
   * Checks if file exists by trying to open and then closes
   * @return boolean of whether file found
   */
  public boolean exists() {
    try {
      this.fileObject = new File(this.filePath);
      this.fileScanner = new Scanner(this.fileObject);
      closeFile();
      return true;
    } catch(FileNotFoundException e) {
      return false;
    }
  }

  /**
   * Reads "next" line in file and appends to ArrayList
   * @return Next line as a string
   */
  public String getNextLine() {
    if (!this.readWholeFile) {
      this.currentLineNumber++;
      if (!this.fileScanner.hasNextLine()) {
        System.err.println("REACHED END OF FILE");
        this.fileScanner.close();
        this.readWholeFile = true;
        return null;
      } else {
        this.fileContentsByLine.add(this.fileScanner.nextLine());
      }
      return this.fileContentsByLine.get(this.currentLineNumber);
    } else {
      System.err.println("REACHED END OF FILE");
      this.fileScanner.close();
      return null;
    }
  }

  /**
   * Reads whole file into ArrayList
   * @return void
   */
  public void readWholeFile() {
    if (!this.fileScanner.hasNextLine()) {
      System.err.println("FILE IS EMPTY");
      this.fileScanner.close();
    } else {
      while(this.fileScanner.hasNextLine()) {
        this.fileContentsByLine.add(this.fileScanner.nextLine());
      }
    }
    this.readWholeFile = true;
    this.currentLineNumber = this.fileContentsByLine.size() - 1;
  }

  /**
   * Accesses line by line number. If line hasn't been read into ArrayList
   * yet, file read and saved up until that point.
   * @param lineNumber Number of line desired (line numbers indexed from 0)
   * @return Desired line as a string
   */
  public String getLine(int lineNumber) {
    // NOTE: LINE NUMBER INDEXED FROM 0
    if (lineNumber > this.currentLineNumber) {
      // Read line-by-line till at desired line
      while (lineNumber > this.currentLineNumber) {
        this.getNextLine();
      }
    }
    return this.fileContentsByLine.get(lineNumber);
  }

  // public static void main(String[] args) {
  //
  //   // TESTING FUNCTIONALITY:
  //
  //   MyFileReader fr = new MyFileReader("../../data/positive.txt");
  //   System.out.println("[ CURRENT LINE: " + fr.getCurrentLineNumber() + " ]");
  //   System.out.println("[ ARRAY LIST SIZE : " + fr.getFileContentsByLine().size() + " ]\n");
  //
  //   System.out.println("getNextLine() ==> " + fr.getNextLine());
  //   System.out.println("fileContentsByLine: " + fr.getFileContentsByLine());
  //   System.out.println("\n[ CURRENT LINE: " + fr.getCurrentLineNumber() + " ]");
  //   System.out.println("[ ARRAY LIST SIZE : " + fr.getFileContentsByLine().size() + " ]\n");
  //
  //   System.out.println("getCurrentLine() ==> " + fr.getCurrentLine());
  //   System.out.println("fileContentsByLine: " + fr.getFileContentsByLine());
  //   System.out.println("\n[ CURRENT LINE: " + fr.getCurrentLineNumber() + " ]");
  //   System.out.println("[ ARRAY LIST SIZE : " + fr.getFileContentsByLine().size() + " ]\n");
  //
  //   System.out.println("getNextLine() ==> " + fr.getNextLine());
  //   System.out.println("fileContentsByLine: " + fr.getFileContentsByLine());
  //   System.out.println("\n[ CURRENT LINE: " + fr.getCurrentLineNumber() + " ]");
  //   System.out.println("[ ARRAY LIST SIZE : " + fr.getFileContentsByLine().size() + " ]\n");
  //
  //   System.out.println("getLine(5) ==> " + fr.getLine(5));
  //   System.out.println("fileContentsByLine: " + fr.getFileContentsByLine());
  //   System.out.println("\n[ CURRENT LINE: " + fr.getCurrentLineNumber() + " ]");
  //   System.out.println("[ ARRAY LIST SIZE : " + fr.getFileContentsByLine().size() + " ]\n");
  //
  //   fr.readWholeFile();
  //   System.out.println("readWholeFile()");
  //   System.out.println("\n[ CURRENT LINE: " + fr.getCurrentLineNumber() + " ]");
  //   System.out.println("[ ARRAY LIST SIZE : " + fr.getFileContentsByLine().size() + " ]\n");
  //
  //   System.out.println("getNextLine() ==> " + fr.getNextLine());
  //   System.out.println("\n[ CURRENT LINE: " + fr.getCurrentLineNumber() + " ]");
  //   System.out.println("[ ARRAY LIST SIZE : " + fr.getFileContentsByLine().size() + " ]\n");
  //
  //   System.out.println("getLine(5) ==> " + fr.getLine(5));
  //   System.out.println("\n[ CURRENT LINE: " + fr.getCurrentLineNumber() + " ]");
  //   System.out.println("[ ARRAY LIST SIZE : " + fr.getFileContentsByLine().size() + " ]\n");
  // }

}
