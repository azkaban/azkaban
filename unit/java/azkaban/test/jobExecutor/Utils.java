package azkaban.test.jobExecutor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class Utils
{

    public static void dumpFile (String filename, String filecontent) 
    throws IOException {
      PrintWriter writer = new PrintWriter(new FileWriter(filename));
      writer.print(filecontent);
      writer.close();
    }
    
    public static void removeFile (String filename) {
      File file = new File (filename);
      file.delete();
    }
    
}
