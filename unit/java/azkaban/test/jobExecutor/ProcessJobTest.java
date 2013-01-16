package azkaban.test.jobExecutor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.log4j.Logger;

import azkaban.utils.Props;
import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.jobExecutor.ProcessJob;


public class ProcessJobTest
{
  private ProcessJob job = null;
//  private JobDescriptor descriptor = null;
  private Props props = null;
  private Logger log = Logger.getLogger(ProcessJob.class);
  @Before
  public void setUp() {
    
    /*  initialize job */
//    props = EasyMock.createMock(Props.class);
    
    props = new Props();
    props.put(AbstractProcessJob.WORKING_DIR, ".");
    props.put("type", "command");
    props.put("fullPath", ".");

    
//    EasyMock.expect(props.getString("type")).andReturn("command").times(1);
//    EasyMock.expect(props.getProps()).andReturn(props).times(1);
//    EasyMock.expect(props.getString("fullPath")).andReturn(".").times(1);
//    
//    EasyMock.replay(props);
    
    job = new ProcessJob("TestProcess", props, props, log);
    

  }
  
  @Test
  public void testOneUnixCommand() throws Exception {
    /* initialize the Props */
    props.put(ProcessJob.COMMAND, "ls -al");
    props.put(ProcessJob.WORKING_DIR, ".");

    job.run();
    
  }

  @Test
  public void testFailedUnixCommand() throws Exception  {
    /* initialize the Props */
    props.put(ProcessJob.COMMAND, "xls -al");
    props.put(ProcessJob.WORKING_DIR, ".");

    try {
      job.run();
    }catch (RuntimeException e) {
      Assert.assertTrue(true);
      e.printStackTrace();
    }
  }
    
    @Test
    public void testMultipleUnixCommands( ) throws Exception  {
      /* initialize the Props */
      props.put(ProcessJob.WORKING_DIR, ".");
      props.put(ProcessJob.COMMAND, "pwd");
      props.put("command.1", "date");
      props.put("command.2", "whoami");
      
      job.run();
    }
    
    @Test
    public void testPartitionCommand() throws Exception {
    	String test1 = "a b c";

    	Assert.assertArrayEquals(new String[] {"a", "b", "c"}, ProcessJob.partitionCommandLine(test1));
    	
    	String test2 = "a 'b c'";
    	Assert.assertArrayEquals(new String[] {"a", "b c"}, ProcessJob.partitionCommandLine(test2));
    	
    	String test3 = "a e='b c'";
    	Assert.assertArrayEquals(new String[] {"a", "e=b c"}, ProcessJob.partitionCommandLine(test3));
    }
}


