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
import sun.net.www.protocol.jar.JarURLConnection;

/**
 * Workaround for jdk 6 disgrace with open jar files & native libs, which is a reason of
 * unrefreshable classloader.
 */
public class ValidatorClassLoader extends URLClassLoader {

  protected HashSet<String> setJarFileNames2Close = new HashSet<>();

  public ValidatorClassLoader(final URL[] urls, final ClassLoader parent) {
    super(urls, parent);
  }

  public ValidatorClassLoader(final URL[] urls) {
    super(urls);
  }

  @Override
  public void close() throws ValidatorManagerException {
    this.setJarFileNames2Close.clear();
    closeClassLoader(this);
    finalizeNativeLibs(this);
    cleanupJarFileFactory();
  }

  /**
   * cleanup jar file factory cache
   */
  public boolean cleanupJarFileFactory() throws ValidatorManagerException {
    boolean res = false;
    final Class classJarURLConnection = JarURLConnection.class;
    Field f;
    try {
      f = classJarURLConnection.getDeclaredField("factory");
    } catch (final NoSuchFieldException e) {
      throw new ValidatorManagerException(e);
    }
    if (f == null) {
      return false;
    }
    f.setAccessible(true);
    Object obj;
    try {
      obj = f.get(null);
    } catch (final IllegalAccessException e) {
      throw new ValidatorManagerException(e);
    }
    if (obj == null) {
      return false;
    }
    final Class classJarFileFactory = obj.getClass();

    HashMap fileCache = null;
    try {
      f = classJarFileFactory.getDeclaredField("fileCache");
      f.setAccessible(true);
      obj = f.get(null);
      if (obj instanceof HashMap) {
        fileCache = (HashMap) obj;
      }
    } catch (NoSuchFieldException | IllegalAccessException e) {
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
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new ValidatorManagerException(e);
    }
    if (urlCache != null) {
      final HashMap urlCacheTmp = (HashMap) urlCache.clone();
      final Iterator it = urlCacheTmp.keySet().iterator();
      while (it.hasNext()) {
        obj = it.next();
        if (!(obj instanceof JarFile)) {
          continue;
        }
        final JarFile jarFile = (JarFile) obj;
        if (this.setJarFileNames2Close.contains(jarFile.getName())) {
          try {
            jarFile.close();
          } catch (final IOException e) {
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
      final HashMap fileCacheTmp = (HashMap) fileCache.clone();
      final Iterator it = fileCacheTmp.keySet().iterator();
      while (it.hasNext()) {
        final Object key = it.next();
        obj = fileCache.get(key);
        if (!(obj instanceof JarFile)) {
          continue;
        }
        final JarFile jarFile = (JarFile) obj;
        if (this.setJarFileNames2Close.contains(jarFile.getName())) {
          try {
            jarFile.close();
          } catch (final IOException e) {
            throw new ValidatorManagerException(e);
          }
          fileCache.remove(key);
        }
      }
      res = true;
    }
    this.setJarFileNames2Close.clear();
    return res;
  }

  /**
   * close jar files of cl
   */
  public boolean closeClassLoader(final ClassLoader cl) throws ValidatorManagerException {
    boolean res = false;
    if (cl == null) {
      return res;
    }
    final Class classURLClassLoader = URLClassLoader.class;
    Field f = null;
    try {
      f = classURLClassLoader.getDeclaredField("ucp");
    } catch (final NoSuchFieldException e) {
      throw new ValidatorManagerException(e);
    }
    if (f != null) {
      f.setAccessible(true);
      Object obj = null;
      try {
        obj = f.get(cl);
      } catch (final IllegalAccessException e) {
        throw new ValidatorManagerException(e);
      }
      if (obj != null) {
        final Object ucp = obj;
        f = null;
        try {
          f = ucp.getClass().getDeclaredField("loaders");
        } catch (final NoSuchFieldException e) {
          throw new ValidatorManagerException(e);
        }
        if (f != null) {
          f.setAccessible(true);
          ArrayList loaders = null;
          try {
            loaders = (ArrayList) f.get(ucp);
            res = true;
          } catch (final IllegalAccessException e) {
            throw new ValidatorManagerException(e);
          }
          for (int i = 0; loaders != null && i < loaders.size(); i++) {
            obj = loaders.get(i);
            f = null;
            try {
              f = obj.getClass().getDeclaredField("jar");
            } catch (final NoSuchFieldException e) {
              throw new ValidatorManagerException(e);
            }
            if (f != null) {
              f.setAccessible(true);
              try {
                obj = f.get(obj);
              } catch (final IllegalAccessException e) {
                throw new ValidatorManagerException(e);
              }
              if (obj instanceof JarFile) {
                final JarFile jarFile = (JarFile) obj;
                this.setJarFileNames2Close.add(jarFile.getName());
                try {
                  jarFile.close();
                } catch (final IOException e) {
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
   */
  public boolean finalizeNativeLibs(final ClassLoader cl) throws ValidatorManagerException {
    boolean res = false;
    final Class classClassLoader = ClassLoader.class;
    java.lang.reflect.Field nativeLibraries = null;
    try {
      nativeLibraries = classClassLoader.getDeclaredField("nativeLibraries");
    } catch (final NoSuchFieldException e) {
      throw new ValidatorManagerException(e);
    }
    if (nativeLibraries == null) {
      return res;
    }
    nativeLibraries.setAccessible(true);
    Object obj = null;
    try {
      obj = nativeLibraries.get(cl);
    } catch (final IllegalAccessException e) {
      throw new ValidatorManagerException(e);
    }
    if (!(obj instanceof Vector)) {
      return res;
    }
    res = true;
    final Vector java_lang_ClassLoader_NativeLibrary = (Vector) obj;
    for (final Object lib : java_lang_ClassLoader_NativeLibrary) {
      java.lang.reflect.Method finalize = null;
      try {
        finalize = lib.getClass().getDeclaredMethod("finalize", new Class[0]);
      } catch (final NoSuchMethodException e) {
        throw new ValidatorManagerException(e);
      }
      if (finalize != null) {
        finalize.setAccessible(true);
        try {
          finalize.invoke(lib, new Object[0]);
        } catch (final IllegalAccessException e) {
          throw new ValidatorManagerException(e);
        } catch (final InvocationTargetException e) {
          throw new ValidatorManagerException(e);
        }
      }
    }
    return res;
  }
}
