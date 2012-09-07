package azkaban.test.jobExecutor;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.easymock.classextension.EasyMock;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import azkaban.utils.Props;
import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.jobExecutor.JavaJob;
import azkaban.jobExecutor.ProcessJob;

public class JavaJobTest
{

  private JavaJob job = null;
//  private JobDescriptor descriptor = null;
  private Props props = null;
  private Logger log = Logger.getLogger(JavaJob.class);
  private static String classPaths ;

  private static final String inputContent = 
    "Quick Change in Strategy for a Bookseller \n" +
    " By JULIE BOSMAN \n" +
    "Published: August 11, 2010 \n" +
    " \n" +
    "Twelve years later, it may be Joe Fox�s turn to worry. Readers have gone from skipping small \n" +
    "bookstores to wondering if they need bookstores at all. More people are ordering books online  \n" +
    "or plucking them from the best-seller bin at Wal-Mart";

  private static final String errorInputContent = 
      inputContent + "\n stop_here " +
      "But the threat that has the industry and some readers the most rattled is the growth of e-books. \n" +
      " In the first five months of 2009, e-books made up 2.9 percent of trade book sales. In the same period \n" +
      "in 2010, sales of e-books, which generally cost less than hardcover books, grew to 8.5 percent, according \n" +
      "to the Association of American Publishers, spurred by sales of the Amazon Kindle and the new Apple iPad. \n" +
      "For Barnes & Noble, long the largest and most powerful bookstore chain in the country, the new competition \n" +
      "has led to declining profits and store traffic.";
  
 
  private static String inputFile ;
  private static String errorInputFile ;
  private static String outputFile ;
  
  @BeforeClass
  public static void init() {
    // get the classpath
    Properties prop = System.getProperties();
    classPaths = String.format("'%s'", prop.getProperty("java.class.path", null));
 
    long time = (new Date()).getTime();
   inputFile = "/tmp/azkaban_input_" + time;
   errorInputFile = "/tmp/azkaban_input_error_" + time;
   outputFile = "/tmp/azkaban_output_" + time;
    // dump input files
   try {
     Utils.dumpFile(inputFile, inputContent);
     Utils.dumpFile(errorInputFile, errorInputContent);
   }
   catch (IOException e) {
     e.printStackTrace(System.err);
     Assert.fail("error in creating input file:" + e.getLocalizedMessage());
   }
        
  }
  
  @AfterClass
  public static void cleanup() {
    // remove the input file and error input file
    Utils.removeFile(inputFile);
    Utils.removeFile(errorInputFile);
    //Utils.removeFile(outputFile);
  }
  
  @Before
  public void setUp() {
    
    /*  initialize job */
//    descriptor = EasyMock.createMock(JobDescriptor.class);
    
    props = new Props();
    props.put(AbstractProcessJob.WORKING_DIR, ".");
    props.put("type", "java");
    props.put("fullPath", ".");
    
//    EasyMock.expect(descriptor.getId()).andReturn("java").times(1);
//    EasyMock.expect(descriptor.getProps()).andReturn(props).times(1);
//    EasyMock.expect(descriptor.getFullPath()).andReturn(".").times(1);
//    
//    EasyMock.replay(descriptor);
    
    job = new JavaJob(props, log);
    
//    EasyMock.verify(descriptor);
  }
  
  @Test
  public void testJavaJob() {
    /* initialize the Props */
    props.put(JavaJob.JOB_CLASS, "azkaban.test.jobExecutor.WordCountLocal");
    props.put(ProcessJob.WORKING_DIR, ".");
    props.put("input", inputFile);
    props.put("output", outputFile);
    props.put("classpath",  classPaths);
    job.run();
  }
  
  @Test
  public void testFailedJavaJob() {
    props.put(JavaJob.JOB_CLASS, "azkaban.test.jobExecutor.WordCountLocal");
    props.put(ProcessJob.WORKING_DIR, ".");
    props.put("input", errorInputFile);
    props.put("output", outputFile);
    props.put("classpath", classPaths);
    
    try {
    job.run();
    }
    catch (RuntimeException e) {
      Assert.assertTrue(true);
    }
  }
  
}

