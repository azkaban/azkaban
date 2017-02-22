/*
 * Copyright 2017 LinkedIn Corp.
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
package azkaban.el;

import azkaban.utils.Props;
import org.apache.log4j.Logger;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ElManager {

    private static  Logger logger = Logger.getLogger(ElManager.class);

    private static final String PROPS_EL_PREFIX = "azkaban.el.func.";
    private static final String EL_LIB_DIR = "azkaban.el.lib";


    private static Map<String,Object> functionMap = new HashMap<String,Object>();
    static {
        functionMap.put(null,BasicFunction.class);
    }



    public static Map<String,Object> getFunctionMap(){
        return functionMap;
    }


    /**
     * configuration format : azkaban.el.func.namespace=package.className
     * eg
     * configuration:
     * azkaban.el.func.math= org.something.Math
     * expression:
     * math:add(6,7)
     * it will call org.something.Math.add
     * @param azkabanProps
     * @param parentClassLoader
     */
    public static void loadElFunctions(Props azkabanProps,ClassLoader parentClassLoader){
        try {
            Map<String, String> externalELMap = azkabanProps.getMapByPrefix(PROPS_EL_PREFIX);

            if (externalELMap.size() > 0) {
                ClassLoader classLoader = elClassLoader(azkabanProps, parentClassLoader);
                for (Map.Entry<String, String> entry : externalELMap.entrySet()) {

                    String key = entry.getKey();
                    String className = entry.getValue();
                    String namespace = key.replaceAll("azkaban.el.func.", "");
                    Class c = classLoader.loadClass(className);
                    functionMap.put(namespace, c);
                }
            }
        }
        catch (Exception e){
            logger.error("load el function error:"+e.getMessage());
        }
    }

    /**
     * from ${path} path load lib into ${resources}
     * @param path
     * @param resources
     * @throws MalformedURLException
     */
    private static  void addToResources(String path,List<URL> resources) throws MalformedURLException {
        if (path.endsWith("/*")) {
            File pFile = new File(path.substring(0, path.length() - 1));
            File files[] = pFile.listFiles();
            for (File file : files) {
                if (file.getName().endsWith(".jar")) {
                    URL cpItem = file.toURI().toURL();
                    if (!resources.contains(cpItem)) {
                        logger.info("adding to classpath " + cpItem);
                        resources.add(cpItem);
                    }
                }
            }
        } else if(path.endsWith(".jar")) {
            URL cpItem = new File(path).toURI().toURL();
            if (!resources.contains(cpItem)) {
                logger.info("adding to classpath " + cpItem);
                resources.add(cpItem);
            }
        }

    }


    /**
     * create classLoader for el function
     * @param azkabanProps
     * @param parentClassLoader
     * @return
     * @throws Exception
     */
    private static ClassLoader elClassLoader(Props azkabanProps,ClassLoader parentClassLoader) throws Exception{
        List<String> elLibDirs = azkabanProps.getStringList(EL_LIB_DIR, null, ",");
        List<URL> resources = new ArrayList<URL>();
        if(elLibDirs!=null) {
            for(String elLibDir:elLibDirs){
                addToResources(elLibDir,resources);
            }
        }
        ClassLoader jobTypeLoader =
                new URLClassLoader(resources.toArray(new URL[resources.size()]),
                        parentClassLoader);
        return jobTypeLoader;
    }
}
