package azkaban.test.utils;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import azkaban.utils.JSONUtils;

public class JsonUtilsTest {
	@Test
	public void writePropsNoJarDependencyTest1() throws IOException {
		Map<String, String> test = new HashMap<String,String>();
		test.put("\"myTest\n\b", "myValue\t\\");
		test.put("normalKey", "Other key");
		
		StringWriter writer = new StringWriter();
		JSONUtils.writePropsNoJarDependency(test, writer);
		
		String jsonStr = writer.toString();
		System.out.println(writer.toString());
		
		@SuppressWarnings("unchecked")
		Map<String,String> result = (Map<String,String>)JSONUtils.parseJSONFromString(jsonStr);
		checkInAndOut(test, result);
	}
	
	@Test
	public void writePropsNoJarDependencyTest2() throws IOException {
		Map<String, String> test = new HashMap<String,String>();
		test.put("\"myTest\n\b", "myValue\t\\");
		
		StringWriter writer = new StringWriter();
		JSONUtils.writePropsNoJarDependency(test, writer);
		
		String jsonStr = writer.toString();
		System.out.println(writer.toString());
		
		@SuppressWarnings("unchecked")
		Map<String,String> result = (Map<String,String>)JSONUtils.parseJSONFromString(jsonStr);
		checkInAndOut(test, result);
	}
	
	private static void checkInAndOut(Map<String, String> before, Map<String, String> after) {
		for (Map.Entry<String, String> entry: before.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			
			String retValue = after.get(key);
			Assert.assertEquals(value, retValue);
		}
	}
	
}