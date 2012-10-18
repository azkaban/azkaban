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

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

public abstract class HdfsSequenceFileViewer implements HdfsFileViewer {

    protected abstract boolean canReadFile(SequenceFile.Reader reader);

    protected abstract void displaySequenceFile(SequenceFile.Reader reader,
                                                PrintWriter output,
                                                int startLine,
                                                int endLine) throws IOException;

    public boolean canReadFile(FileSystem fs, Path file) {
        boolean result = false;
        try {
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, new Configuration());
            result = canReadFile(reader);
            reader.close();
        } catch(IOException e) {
            return false;
        }

        return result;
    }

    public void displayFile(FileSystem fs,
                            Path file,
                            OutputStream outputStream,
                            int startLine,
                            int endLine) throws IOException {
        SequenceFile.Reader reader = null;
        PrintWriter writer = new PrintWriter(outputStream);
        try {
            reader = new SequenceFile.Reader(fs, file, new Configuration());
            displaySequenceFile(reader, writer, startLine, endLine);
        } catch(IOException e) {
            writer.write("Error opening sequence file " + e);
        } finally {
            if(reader != null) {
                reader.close();
            }
        }
    }
}