/*
 * Copyright 2012 LinkedIn Corp.
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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
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
		} else {
			return t;
		}
	}

	/**
	 * Print the message and then exit with the given exit code
	 * 
	 * @param message
	 *            The message to print
	 * @param exitCode
	 *            The exit code
	 */
	public static void croak(String message, int exitCode) {
		System.err.println(message);
		System.exit(exitCode);
	}

	public static File createTempDir() {
		return createTempDir(new File(System.getProperty("java.io.tmpdir")));
	}

	public static File createTempDir(File parent) {
		File temp = new File(parent,
				Integer.toString(Math.abs(RANDOM.nextInt()) % 100000000));
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

	public static void zipFolderContent(File folder, File output) throws IOException {
		FileOutputStream out = new FileOutputStream(output);
		ZipOutputStream zOut = new ZipOutputStream(out);
		File[] files = folder.listFiles();
		if (files != null) {
			for (File f : files) {
				zipFile("", f, zOut);
			}
		}
		zOut.close();
	}

	private static void zipFile(String path, File input, ZipOutputStream zOut) throws IOException {
		if (input.isDirectory()) {
			File[] files = input.listFiles();
			if (files != null) {
				for (File f : files) {
					String childPath = path + input.getName()
							+ (f.isDirectory() ? "/" : "");
					zipFile(childPath, f, zOut);
				}
			}
		} else {
			String childPath = path + (path.length() > 0 ? "/" : "")
					+ input.getName();
			ZipEntry entry = new ZipEntry(childPath);
			zOut.putNextEntry(entry);
			InputStream fileInputStream = new BufferedInputStream(
					new FileInputStream(input));
			IOUtils.copy(fileInputStream, zOut);
			fileInputStream.close();
		}
	}

	public static void unzip(ZipFile source, File dest) throws IOException {
		Enumeration<?> entries = source.entries();
		while (entries.hasMoreElements()) {
			ZipEntry entry = (ZipEntry) entries.nextElement();
			File newFile = new File(dest, entry.getName());
			if (entry.isDirectory()) {
				newFile.mkdirs();
			} else {
				newFile.getParentFile().mkdirs();
				InputStream src = source.getInputStream(entry);
				OutputStream output = new BufferedOutputStream(
						new FileOutputStream(newFile));
				IOUtils.copy(src, output);
				src.close();
				output.close();
			}
		}
	}

	public static String flattenToString(Collection<?> collection, String delimiter) {
		StringBuffer buffer = new StringBuffer();
		for (Object obj : collection) {
			buffer.append(obj.toString());
			buffer.append(',');
		}

		if (buffer.length() > 0) {
			buffer.setLength(buffer.length() - 1);
		}
		return buffer.toString();
	}

	public static Double convertToDouble(Object obj) {
		if (obj instanceof String) {
			return Double.parseDouble((String) obj);
		}

		return (Double) obj;
	}
	
    /**
     * Get the root cause of the Exception
     * 
     * @param e The Exception
     * @return The root cause of the Exception
     */
    private static RuntimeException getCause(InvocationTargetException e) {
        Throwable cause = e.getCause();
        if(cause instanceof RuntimeException)
            throw (RuntimeException) cause;
        else
            throw new IllegalStateException(e.getCause());
    }
	
    /**
     * Get the Class of all the objects
     * 
     * @param args The objects to get the Classes from
     * @return The classes as an array
     */
    public static Class<?>[] getTypes(Object... args) {
        Class<?>[] argTypes = new Class<?>[args.length];
        for(int i = 0; i < argTypes.length; i++)
            argTypes[i] = args[i].getClass();
        return argTypes;
    }
	
    public static Object callConstructor(Class<?> c, Object... args) {
        return callConstructor(c, getTypes(args), args);
    }

    /**
     * Call the class constructor with the given arguments
     * 
     * @param c The class
     * @param args The arguments
     * @return The constructed object
     */
    public static Object callConstructor(Class<?> c, Class<?>[] argTypes, Object[] args) {
        try {
            Constructor<?> cons = c.getConstructor(argTypes);
            return cons.newInstance(args);
        } catch(InvocationTargetException e) {
            throw getCause(e);
        } catch(IllegalAccessException e) {
            throw new IllegalStateException(e);
        } catch(NoSuchMethodException e) {
            throw new IllegalStateException(e);
        } catch(InstantiationException e) {
            throw new IllegalStateException(e);
        }
    }

	public static String formatDuration(long startTime, long endTime) {
		if (startTime == -1) {
			return "-";
		}
		
		long durationMS;
		if (endTime == -1) {
			durationMS = System.currentTimeMillis() - startTime;
		}
		else {
			durationMS = endTime - startTime;
		}
		
		long seconds = durationMS/1000;
		if (seconds < 60) {
			return seconds + " sec";
		}
		
		long minutes = seconds / 60;
		seconds %= 60;
		if (minutes < 60) {
			return minutes + "m " + seconds + "s";
		}
		
		long hours = minutes / 60;
		minutes %= 60;
		if (hours < 24) {
			return hours + "h " + minutes + "m " + seconds + "s";
		}
		
		long days = hours / 24;
		hours %= 24;
		return days + "d " + hours + "h " + minutes + "m";
	}
	
	public static Object invokeStaticMethod(ClassLoader loader, String className, String methodName, Object ... args) throws ClassNotFoundException, SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		Class<?> clazz = loader.loadClass(className);
		
		Class<?>[] argTypes = new Class[args.length];
		for (int i=0; i < args.length; ++i) {
			argTypes[i] = args[i].getClass();
		}
		
		Method method = clazz.getDeclaredMethod(methodName, argTypes);
		return method.invoke(null, args);
	}
}
