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

package azkaban.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;

/**
 * A util helper class full of static methods that are commonly used.
 */
public class Utils {
    public static final Random RANDOM = new Random();
	
    /**
     * Private constructor.
     */
    private Utils() {
    }

    /**
     * Equivalent to Object.equals except that it handles nulls. If a and b are
     * both null, true is returned.
     * 
     * @param a
     * @param b
     * @return
     */
    public static boolean equals(Object a, Object b) {
        if (a == null || b == null) {
            return a == b;
        }

        return a.equals(b);
    }

    /**
     * Return the object if it is non-null, otherwise throw an exception
     * 
     * @param <T>
     *            The type of the object
     * @param t
     *            The object
     * @return The object if it is not null
     * @throws IllegalArgumentException
     *             if the object is null
     */
    public static <T> T nonNull(T t) {
        if (t == null) {
            throw new IllegalArgumentException("Null value not allowed.");
        }
        else {
            return t;
        }
    }
    
    /**
     * Print the message and then exit with the given exit code
     * 
     * @param message The message to print
     * @param exitCode The exit code
     */
    public static void croak(String message, int exitCode) {
        System.err.println(message);
        System.exit(exitCode);
    }
    
    public static File createTempDir() {
        return createTempDir(new File(System.getProperty("java.io.tmpdir")));
    }

    public static File createTempDir(File parent) {
        File temp = new File(parent, Integer.toString(Math.abs(RANDOM.nextInt()) % 100000000));
        temp.delete();
        temp.mkdir();
        temp.deleteOnExit();
        return temp;
    }
    
    public static void zip(File input, File output) throws IOException {
        FileOutputStream out = new FileOutputStream(output);
        ZipOutputStream zOut = new ZipOutputStream(out);
        zipFile("", input, zOut);
        zOut.close();
    }

    private static void zipFile(String path, File input, ZipOutputStream zOut) throws IOException {
        if(input.isDirectory()) {
            File[] files = input.listFiles();
            if(files != null) {
                for(File f: files) {
                    String childPath = path + input.getName() + (f.isDirectory() ? "/" : "");
                    zipFile(childPath, f, zOut);
                }
            }
        } else {
            String childPath = path + (path.length() > 0 ? "/" : "") + input.getName();
            ZipEntry entry = new ZipEntry(childPath);
            zOut.putNextEntry(entry);
            InputStream fileInputStream = new BufferedInputStream(new FileInputStream(input));
            IOUtils.copy(fileInputStream, zOut);
            fileInputStream.close();
        }
    }

    public static void unzip(ZipFile source, File dest) throws IOException {
        Enumeration<?> entries = source.entries();
        while(entries.hasMoreElements()) {
            ZipEntry entry = (ZipEntry) entries.nextElement();
            File newFile = new File(dest, entry.getName());
            if(entry.isDirectory()) {
                newFile.mkdirs();
            } else {
                newFile.getParentFile().mkdirs();
                InputStream src = source.getInputStream(entry);
                OutputStream output = new BufferedOutputStream(new FileOutputStream(newFile));
                IOUtils.copy(src, output);
                src.close();
                output.close();
            }
        }
    }

}