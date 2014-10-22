package azkaban.project.validator;

import java.io.File;
import java.io.FilenameFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.base.Splitter;

/**
 * A {@link URLClassLoader} for YARN application isolation. Classes from
 * the application JARs are loaded in preference to the parent loader.
 */
public class ValidatorClassLoader extends URLClassLoader {

  private static final Logger logger = Logger.getLogger(XmlValidatorManager.class);

  private static final FilenameFilter JAR_FILENAME_FILTER =
    new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".jar") || name.endsWith(".JAR");
      }
  };

  private ClassLoader parent;

  public ValidatorClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
    this.parent = parent;
    if (parent == null) {
      throw new IllegalArgumentException("No parent classloader!");
    }
  }

  public ValidatorClassLoader(String classpath, ClassLoader parent)
      throws MalformedURLException {
    this(constructUrlsFromClasspath(classpath), parent);
  }

  static URL[] constructUrlsFromClasspath(String classpath)
      throws MalformedURLException {
    List<URL> urls = new ArrayList<URL>();
    for (String element : Splitter.on(File.pathSeparator).split(classpath)) {
      if (element.endsWith("/*")) {
        String dir = element.substring(0, element.length() - 1);
        File[] files = new File(dir).listFiles(JAR_FILENAME_FILTER);
        if (files != null) {
          for (File file : files) {
            urls.add(file.toURI().toURL());
          }
        }
      } else {
        File file = new File(element);
        if (file.exists()) {
          urls.add(new File(element).toURI().toURL());
        }
      }
    }
    return urls.toArray(new URL[urls.size()]);
  }

  @Override
  public URL getResource(String name) {
    URL url = null;

    url= findResource(name);
    if (url == null && name.startsWith("/")) {
      if (logger.isDebugEnabled()) {
        logger.debug("Remove leading / off " + name);
      }
      url= findResource(name.substring(1));
    }

    if (url == null) {
      url= parent.getResource(name);
    }

    if (url != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("getResource("+name+")=" + url);
      }
    }

    return url;
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    return this.loadClass(name, false);
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException {

    if (logger.isDebugEnabled()) {
      logger.debug("Loading class: " + name);
    }

    Class<?> c = findLoadedClass(name);
    ClassNotFoundException ex = null;

    if (c == null) {
      // Try to load class from this classloader's URLs. Note that this is like
      // the servlet spec, not the usual Java 2 behaviour where we ask the
      // parent to attempt to load first.
      try {
        c = findClass(name);
        if (logger.isDebugEnabled() && c != null) {
          logger.debug("Loaded class: " + name + " ");
        }
      } catch (ClassNotFoundException e) {
        if (logger.isDebugEnabled()) {
          logger.debug(e);
        }
        ex = e;
      }
    }

    if (c == null) { // try parent
      c = parent.loadClass(name);
      if (logger.isDebugEnabled() && c != null) {
        logger.debug("Loaded class from parent: " + name + " ");
      }
    }

    if (c == null) {
      throw ex != null ? ex : new ClassNotFoundException(name);
    }

    if (resolve) {
      resolveClass(c);
    }

    return c;
  }
}
