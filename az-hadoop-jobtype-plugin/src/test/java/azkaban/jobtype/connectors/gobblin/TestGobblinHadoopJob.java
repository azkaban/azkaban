package azkaban.jobtype.connectors.gobblin;


public class TestGobblinHadoopJob {
//  private static final String JOB_ID = "test_job_id";
//  private static final Logger LOG = Logger.getLogger(TestGobblinHadoopJob.class);
//
//  @Test
//  public void testPrintableJobProperties() {
//    Set<String> passWordKeys = Sets.newHashSet("source.conn.password", "jdbc.publisher.password", "password", "pass_word", "pass.word", "passWord");

//    Props sysPros = new Props();
//    //Add dummy directory path so that GobblinHadoopJob can be instantiated.
//    sysPros.put(GobblinConstants.GOBBLIN_PRESET_DIR_KEY, this.getClass()
//                                                             .getProtectionDomain()
//                                                             .getCodeSource()
//                                                             .getLocation()
//                                                             .getPath() + "/" + "dummy");
//    Props jobPros = new Props();
//
//    GobblinHadoopJob job = new GobblinHadoopJob(JOB_ID, sysPros, jobPros, LOG);
//    Set<String> normalKeys = Sets.newHashSet(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString());
//
//    Props props = new Props();
//    putDummyValue(props, normalKeys);
//    Assert.assertTrue(job.printableJobProperties(props).keySet().containsAll(normalKeys) && job.printableJobProperties(props).keySet().size() == normalKeys.size());
//
//    putDummyValue(props, passWordKeys);
//    //Adding password keys should not change output of printableJobProperties
//    Assert.assertTrue(job.printableJobProperties(props).keySet().containsAll(normalKeys) && job.printableJobProperties(props).keySet().size() == normalKeys.size());
//  }
//
//  private void putDummyValue(Props props, Set<String> keys) {
//    for (String key : keys) {
//      props.put(key, "dummy");
//    }
//  }
}
