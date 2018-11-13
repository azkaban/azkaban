/*
 * Copyright 2018 LinkedIn Corp.
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

package azkaban.flowtrigger.plugin;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * A parent-last classloader that will try the child classloader first and then the parent.
 * Adopted from https://stackoverflow.com/questions/5445511/how-do-i-create-a-parent-last-child-first-classloader-in-java-or-how-to-overr
 */
public class ParentLastURLClassLoader extends ClassLoader {

  private final ChildURLClassLoader childClassLoader;

  public ParentLastURLClassLoader(final URL[] urls, final ClassLoader parentCL) {
    super(parentCL);

    this.childClassLoader = new ChildURLClassLoader(urls,
        new FindClassClassLoader(this.getParent()));
  }

  @Override
  protected synchronized Class<?> loadClass(final String name, final boolean resolve)
      throws ClassNotFoundException {
    try {
      // first we try to find a class inside the child classloader
      return this.childClassLoader.findClass(name);
    } catch (final ClassNotFoundException e) {
      // didn't find it, try the parent
      return super.loadClass(name, resolve);
    }
  }

  /**
   * This class allows me to call findClass on a classloader
   */
  private static class FindClassClassLoader extends ClassLoader {

    public FindClassClassLoader(final ClassLoader parent) {
      super(parent);
    }

    @Override
    public Class<?> findClass(final String name) throws ClassNotFoundException {
      return super.findClass(name);
    }
  }

  /**
   * This class delegates (child then parent) for the findClass method for a URLClassLoader.
   * We need this because findClass is protected in URLClassLoader
   */
  private static class ChildURLClassLoader extends URLClassLoader {

    private final FindClassClassLoader realParent;

    public ChildURLClassLoader(final URL[] urls, final FindClassClassLoader realParent) {
      super(urls, null);

      this.realParent = realParent;
    }

    @Override
    public Class<?> findClass(final String name) throws ClassNotFoundException {
      try {
        final Class<?> loaded = super.findLoadedClass(name);
        if (loaded != null) {
          return loaded;
        }

        // first try to use the URLClassLoader findClass
        return super.findClass(name);
      } catch (final ClassNotFoundException e) {
        // if that fails, we ask our real parent classloader to load the class (we give up)
        return this.realParent.loadClass(name);
      }
    }
  }
}
