package azkaban.jobtype;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class TestHadoopSecureHiveWrapper {
  // Ensure that hivevars with equal signs in them are parsed
  // properly, as the string may be split incorrectly around them.
  @Test
  public void testParamsWithEqualSigns() {
    String[] args = {"-hivevar", "'testKey1=testVal1'",
        "-hivevar", "'testKey2=testVal2==something=anything'",
        "-hivevar", "'testKey3=testVal3=anything'"};
    Map<String, String> hiveVarMap = HadoopSecureHiveWrapper.getHiveVarMap(args);
    Assert.assertTrue(hiveVarMap.size() == 3);
    Assert.assertTrue(hiveVarMap.get("testKey1").equals("testVal1"));
    Assert.assertTrue(hiveVarMap.get("testKey2").equals("testVal2==something=anything"));
    Assert.assertTrue(hiveVarMap.get("testKey3").equals("testVal3=anything"));
  }
}

