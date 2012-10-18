/*
 * Copyright 2012 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.fsviewers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class TextFileViewer implements HdfsFileViewer {

    private static Logger logger = Logger.getLogger(TextFileViewer.class);
    private HashSet<String> acceptedSuffix = new HashSet<String>();

    public TextFileViewer() {
        acceptedSuffix.add(".txt");
        acceptedSuffix.add(".csv");
        acceptedSuffix.add(".props");
        acceptedSuffix.add(".xml");
        acceptedSuffix.add(".html");
        acceptedSuffix.add(".json");
        acceptedSuffix.add(".log");
    }

    public boolean canReadFile(FileSystem fs, Path path) {
        return true;
    }

    public void displayFile(FileSystem fs,
                            Path path,
                            OutputStream outputStream,
                            int startLine,
                            int endLine) throws IOException {

        if(logger.isDebugEnabled())
            logger.debug("read in uncompressed text file");
        InputStream inputStream = fs.open(path);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        PrintWriter output = new PrintWriter(outputStream);
        for(int i = 1; i < startLine; i++)
            reader.readLine();
        
        final int bufferLimit = 1000000; //only display the first 1M chars. it is used to prevent showing/downloading gb of data
        int bufferSize = 0;
        for(int i = startLine; i < endLine; i++) {
            String line = reader.readLine();
            if(line == null)
                break;
            
            // bread if reach the buffer limit
            bufferSize += line.length();
            if (bufferSize >= bufferLimit)
                break;
            
            output.write(line);
            output.write("\n");
        }
        output.flush();
        reader.close();
    }
}