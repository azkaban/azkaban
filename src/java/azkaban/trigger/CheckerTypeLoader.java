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

package azkaban.trigger;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import azkaban.utils.Props;
import azkaban.utils.Utils;


public class CheckerTypeLoader {
	
	private static Logger logger = Logger.getLogger(CheckerTypeLoader.class);
	
	public static final String DEFAULT_CONDITION_CHECKER_PLUGIN_DIR = "plugins/conditioncheckers";
	
	protected static Map<String, Class<? extends ConditionChecker>> checkerToClass = new HashMap<String, Class<? extends ConditionChecker>>();
	
	public void init(Props props) throws TriggerException {
		
		
		// load built-in checkers
//		
//		loadBuiltinCheckers();
//		
//		loadPluginCheckers(props);

	}
	
	public synchronized void registerCheckerType(String type, Class<? extends ConditionChecker> checkerClass) {
		logger.info("Registering checker " + type);
		if(!checkerToClass.containsKey(type)) {
			checkerToClass.put(type, checkerClass);
		}
	}
	
//	private void loadPluginCheckers(Props props) throws TriggerException {
//		
//		String checkerDir = props.getString("azkaban.condition.checker.plugin.dir", DEFAULT_CONDITION_CHECKER_PLUGIN_DIR);
//		File pluginDir = new File(checkerDir);
//		if(!pluginDir.exists() || !pluginDir.isDirectory() || !pluginDir.canRead()) {
//			logger.info("No conditon checker plugins to load.");
//			return;
//		}
//		
//		logger.info("Loading plugin condition checkers from " + pluginDir);
//		ClassLoader parentCl = this.getClass().getClassLoader();
//		
//		Props globalCheckerConf = null;
//		File confFile = Utils.findFilefromDir(pluginDir, COMMONCONFFILE);
//		try {
//			if(confFile != null) {
//				globalCheckerConf = new Props(null, confFile);
//			} else {
//				globalCheckerConf = new Props();
//			}
//		} catch (IOException e) {
//			throw new TriggerException("Failed to get global properties." + e);
//		}
//		
//		for(File dir : pluginDir.listFiles()) {
//			if(dir.isDirectory() && dir.canRead()) {
//				try {
//					loadPluginTypes(globalCheckerConf, pluginDir, parentCl);
//				} catch (Exception e) {
//					logger.info("Plugin checkers failed to load. " + e.getCause());
//					throw new TriggerException("Failed to load all condition checkers!", e);
//				}
//			}
//		}
//	}
//	
//	@SuppressWarnings("unchecked")
//	private void loadPluginTypes(Props globalConf, File dir, ClassLoader parentCl) throws TriggerException {
//		Props checkerConf = null;
//		File confFile = Utils.findFilefromDir(dir, CHECKERTYPECONFFILE);
//		if(confFile == null) {
//			logger.info("No checker type found in " + dir.getAbsolutePath());
//			return;
//		}
//		try {
//			checkerConf = new Props(globalConf, confFile);
//		} catch (IOException e) {
//			throw new TriggerException("Failed to load config for the checker type", e);
//		}
//		
//		String checkerName = dir.getName();
//		String checkerClass = checkerConf.getString("checker.class");
//		
//		List<URL> resources = new ArrayList<URL>();		
//		for(File f : dir.listFiles()) {
//			try {
//				if(f.getName().endsWith(".jar")) {
//					resources.add(f.toURI().toURL());
//					logger.info("adding to classpath " + f.toURI().toURL());
//				}
//			} catch (MalformedURLException e) {
//				// TODO Auto-generated catch block
//				throw new TriggerException(e);
//			}
//		}
//		
//		// each job type can have a different class loader
//		ClassLoader checkerCl = new URLClassLoader(resources.toArray(new URL[resources.size()]), parentCl);
//		
//		Class<? extends ConditionChecker> clazz = null;
//		try {
//			clazz = (Class<? extends ConditionChecker>)checkerCl.loadClass(checkerClass);
//			checkerToClass.put(checkerName, clazz);
//		}
//		catch (ClassNotFoundException e) {
//			throw new TriggerException(e);
//		}
//		
//		if(checkerConf.getBoolean("need.init")) {
//			try {
//				Utils.invokeStaticMethod(checkerCl, checkerClass, "init", checkerConf);
//			} catch (Exception e) {
//				e.printStackTrace();
//				logger.error("Failed to init the checker type " + checkerName);
//				throw new TriggerException(e);
//			}
//		}
//		
//		logger.info("Loaded checker type " + checkerName + " " + checkerClass);
//	}
	
	public static void registerBuiltinCheckers(Map<String, Class<? extends ConditionChecker>> builtinCheckers) {
		checkerToClass.putAll(checkerToClass);
		for(String type : builtinCheckers.keySet()) {
			logger.info("Loaded " + type + " checker.");
		}
	}
	
//	private void loadBuiltinCheckers() {
//		checkerToClass.put("BasicTimeChecker", BasicTimeChecker.class);
//		logger.info("Loaded BasicTimeChecker type.");
//	}
	
	public ConditionChecker createCheckerFromJson(String type, Object obj) throws Exception {
		ConditionChecker checker = null;
		Class<? extends ConditionChecker> checkerClass = checkerToClass.get(type);	
		if(checkerClass == null) {
			throw new Exception("Checker type " + type + " not supported!");
		}
		checker = (ConditionChecker) Utils.invokeStaticMethod(checkerClass.getClassLoader(), checkerClass.getName(), "createFromJson", obj);
		
		return checker;
	}
	
	public ConditionChecker createChecker(String type, Object ... args) {
		ConditionChecker checker = null;
		Class<? extends ConditionChecker> checkerClass = checkerToClass.get(type);		
		checker = (ConditionChecker) Utils.callConstructor(checkerClass, args);
		
		return checker;
	}
	
	public Map<String, Class<? extends ConditionChecker>> getSupportedCheckers() {
		return checkerToClass;
	}
	
}
