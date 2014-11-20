package azkaban.project.validator;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;
import java.util.jar.JarFile;

/**
 * Workaround for jdk 6 disgrace with open jar files & native libs,
 * which is a reason of unrefreshable classloader.
 */
public class ValidatorClassLoader extends URLClassLoader {

  protected HashSet<String> setJarFileNames2Close = new HashSet<String>();

  public ValidatorClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }

  public ValidatorClassLoader(URL[] urls) {
    super(urls);
  }

  public void close() throws ValidatorManagerException {
    setJarFileNames2Close.clear();
    closeClassLoader(this);
    finalizeNativeLibs(this);
    cleanupJarFileFactory();
  }

  /**
   * cleanup jar file factory cache
   */
  @SuppressWarnings({ "nls", "rawtypes" })
  public boolean cleanupJarFileFactory() throws ValidatorManagerException {
    boolean res = false;
    Class classJarURLConnection = null;
    try {
      classJarURLConnection = Class.forName("sun.net.www.protocol.jar.JarURLConnection");
    } catch (ClassNotFoundException e) {
      throw new ValidatorManagerException(e);
    }
    if (classJarURLConnection == null) {
      return res;
    }
    Field f = null;
    try {
      f = classJarURLConnection.getDeclaredField("factory");
    } catch (NoSuchFieldException e) {
      throw new ValidatorManagerException(e);
    }
    if (f == null) {
      return res;
    }
    f.setAccessible(true);
    Object obj = null;
    try {
      obj = f.get(null);
    } catch (IllegalAccessException e) {
      throw new ValidatorManagerException(e);
    }
    if (obj == null) {
      return res;
    }
    Class classJarFileFactory = obj.getClass();

    HashMap fileCache = null;
    try {
      f = classJarFileFactory.getDeclaredField("fileCache");
      f.setAccessible(true);
      obj = f.get(null);
      if (obj instanceof HashMap) {
        fileCache = (HashMap) obj;
      }
    } catch (NoSuchFieldException e) {
      throw new ValidatorManagerException(e);
    } catch (IllegalAccessException e) {
      throw new ValidatorManagerException(e);
    }
    HashMap urlCache = null;
    try {
      f = classJarFileFactory.getDeclaredField("urlCache");
      f.setAccessible(true);
      obj = f.get(null);
      if (obj instanceof HashMap) {
        urlCache = (HashMap) obj;
      }
    } catch (NoSuchFieldException e) {
      throw new ValidatorManagerException(e);
    } catch (IllegalAccessException e) {
      throw new ValidatorManagerException(e);
    }
    if (urlCache != null) {
      HashMap urlCacheTmp = (HashMap) urlCache.clone();
      Iterator it = urlCacheTmp.keySet().iterator();
      while (it.hasNext()) {
        obj = it.next();
        if (!(obj instanceof JarFile)) {
          continue;
        }
        JarFile jarFile = (JarFile) obj;
        if (setJarFileNames2Close.contains(jarFile.getName())) {
          try {
            jarFile.close();
          } catch (IOException e) {
            throw new ValidatorManagerException(e);
          }
          if (fileCache != null) {
            fileCache.remove(urlCache.get(jarFile));
          }
          urlCache.remove(jarFile);
        }
      }
      res = true;
    } else if (fileCache != null) {
      HashMap fileCacheTmp = (HashMap) fileCache.clone();
      Iterator it = fileCacheTmp.keySet().iterator();
      while (it.hasNext()) {
        Object key = it.next();
        obj = fileCache.get(key);
        if (!(obj instanceof JarFile)) {
          continue;
        }
        JarFile jarFile = (JarFile) obj;
        if (setJarFileNames2Close.contains(jarFile.getName())) {
          try {
            jarFile.close();
          } catch (IOException e) {
            throw new ValidatorManagerException(e);
          }
          fileCache.remove(key);
        }
      }
      res = true;
    }
    setJarFileNames2Close.clear();
    return res;
  }

  /**
   * close jar files of cl
   * @param cl
   * @return
   */
  @SuppressWarnings({ "nls", "rawtypes" })
  public boolean closeClassLoader(ClassLoader cl) throws ValidatorManagerException {
    boolean res = false;
    if (cl == null) {
      return res;
    }
    Class classURLClassLoader = URLClassLoader.class;
    Field f = null;
    try {
      f = classURLClassLoader.getDeclaredField("ucp");
    } catch (NoSuchFieldException e) {
      throw new ValidatorManagerException(e);
    }
    if (f != null) {
      f.setAccessible(true);
      Object obj = null;
      try {
        obj = f.get(cl);
      } catch (IllegalAccessException e) {
        throw new ValidatorManagerException(e);
      }
      if (obj != null) {
        final Object ucp = obj;
        f = null;
        try {
          f = ucp.getClass().getDeclaredField("loaders");
        } catch (NoSuchFieldException e) {
          throw new ValidatorManagerException(e);
        }
        if (f != null) {
          f.setAccessible(true);
          ArrayList loaders = null;
          try {
            loaders = (ArrayList) f.get(ucp);
            res = true;
          } catch (IllegalAccessException e) {
            throw new ValidatorManagerException(e);
          }
          for (int i = 0; loaders != null && i < loaders.size(); i++) {
            obj = loaders.get(i);
            f = null;
            try {
              f = obj.getClass().getDeclaredField("jar");
            } catch (NoSuchFieldException e) {
              throw new ValidatorManagerException(e);
            }
            if (f != null) {
              f.setAccessible(true);
              try {
                obj = f.get(obj);
              } catch (IllegalAccessException e) {
                throw new ValidatorManagerException(e);
              }
              if (obj instanceof JarFile) {
                final JarFile jarFile = (JarFile) obj;
                setJarFileNames2Close.add(jarFile.getName());
                try {
                  jarFile.close();
                } catch (IOException e) {
                  throw new ValidatorManagerException(e);
                }
              }
            }
          }
        }
      }
    }
    return res;
  }

  /**
   * finalize native libraries
   * @param cl
   * @return
   */
  @SuppressWarnings({ "nls", "rawtypes" })
  public boolean finalizeNativeLibs(ClassLoader cl) throws ValidatorManagerException {
    boolean res = false;
    Class classClassLoader = ClassLoader.class;
    java.lang.reflect.Field nativeLibraries = null;
    try {
      nativeLibraries = classClassLoader.getDeclaredField("nativeLibraries");
    } catch (NoSuchFieldException e) {
      throw new ValidatorManagerException(e);
    }
    if (nativeLibraries == null) {
      return res;
    }
    nativeLibraries.setAccessible(true);
    Object obj = null;
    try {
      obj = nativeLibraries.get(cl);
    } catch (IllegalAccessException e) {
      throw new ValidatorManagerException(e);
    }
    if (!(obj instanceof Vector)) {
      return res;
    }
    res = true;
    Vector java_lang_ClassLoader_NativeLibrary = (Vector) obj;
    for (Object lib : java_lang_ClassLoader_NativeLibrary) {
      java.lang.reflect.Method finalize = null;
      try {
        finalize = lib.getClass().getDeclaredMethod("finalize", new Class[0]);
      } catch (NoSuchMethodException e) {
        throw new ValidatorManagerException(e);
      }
      if (finalize != null) {
        finalize.setAccessible(true);
        try {
          finalize.invoke(lib, new Object[0]);
        } catch (IllegalAccessException e) {
          throw new ValidatorManagerException(e);
        } catch (InvocationTargetException e) {
          throw new ValidatorManagerException(e);
        }
      }
    }
    return res;
  }
}
