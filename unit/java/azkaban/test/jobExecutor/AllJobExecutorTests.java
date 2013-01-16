package azkaban.test.jobExecutor;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ JavaProcessJobTest.class, ProcessJobTest.class, PythonJobTest.class })
public class AllJobExecutorTests {

}
