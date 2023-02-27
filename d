  @Test
  public void testPopulatingProxyUsersFromProject() throws Exception {
    final ExecutableFlow flow = createTestFlow();
    flow.setProjectId(1);
    final Props flowProps = new Props();
    ProjectManager projectManager = mock(ProjectManager.class);
    Project project = mock(Project.class);
    Flow flowObj = mock(Flow.class);
    HashSet<String> proxyUsers = new HashSet<>();

    ExecutableNode node1 = new ExecutableNode();
    node1.setId("node1");
    node1.setJobSource("job1");
    node1.setStatus(Status.PREPARING);
    Props currentNodeProps1 = mock(Props.class);
    Props currentNodeJobProps1 = mock(Props.class);


    when(projectManager.getProject(flow.getProjectId())).thenReturn(project);
    when(project.getFlow(flow.getFlowId())).thenReturn(flowObj);
    when(projectManager.getProperties(project, flowObj, node1.getId(), node1.getJobSource()))
        .thenReturn(currentNodeProps1);
    when(currentNodeProps1.getString("user.to.proxy", null)).thenReturn("testUser1");
    when(projectManager.getJobOverrideProperty(project, flowObj, node1.getId(),
        node1.getJobSource()))
        .thenReturn(currentNodeJobProps1);
    populateProxyUsersForFlow(flow, node1, projectManager, proxyUsers);

    // First test when there's no job override user.
    Assert.assertTrue(proxyUsers.contains("testUser1"));
    Assert.assertEquals(1, proxyUsers.size());
    proxyUsers.clear();
    when(currentNodeJobProps1.getString("user.to.proxy", null)).thenReturn("overrideUser");
    populateProxyUsersForFlow(flow, node1, projectManager, proxyUsers);

    // Second test when there is a job override user.
    Assert.assertTrue(proxyUsers.contains("overrideUser"));
    Assert.assertEquals(1, proxyUsers.size());

    // Third test : Adding a second node and testing size of proxy user list to test it has
    // overrideUser and testUser2
    ExecutableNode node2 = new ExecutableNode();
    node2.setId("node2");
    node2.setJobSource("job2");
    node2.setStatus(Status.PREPARING);
    Props currentNodeProps2 = mock(Props.class);
    Props currentNodeJobProps2 = mock(Props.class);
    when(projectManager.getProperties(project, flowObj, node2.getId(), node2.getJobSource()))
        .thenReturn(currentNodeProps2);
    when(currentNodeProps2.getString("user.to.proxy", null)).thenReturn("testUser2");
    when(projectManager.getJobOverrideProperty(project, flowObj, node2.getId(),
        node2.getJobSource()))
        .thenReturn(currentNodeJobProps2);
    populateProxyUsersForFlow(flow, node2, projectManager, proxyUsers);

    Assert.assertTrue(proxyUsers.contains("testUser2"));
    Assert.assertEquals(2, proxyUsers.size());
  }

  @Test
  public void testPopulatingJobTypeUsersForFlow() throws Exception {
    Set<String> proxyUsers;
    TreeSet<String> jobTypes = new TreeSet<>();
    jobTypes.add("jobtype1");
    proxyUsers = getJobTypeUsersForFlow(JOBTYPE_PROXY_USER_MAP, jobTypes);
    Assert.assertTrue(proxyUsers.contains("jobtype1_proxyuser"));
    Assert.assertEquals(1, proxyUsers.size());

    jobTypes.add("jobtype2");
    proxyUsers = getJobTypeUsersForFlow(JOBTYPE_PROXY_USER_MAP, jobTypes);
    Assert.assertTrue(proxyUsers.contains("jobtype1_proxyuser"));
    Assert.assertTrue(proxyUsers.contains("jobtype2_proxyuser"));
    Assert.assertEquals(2, proxyUsers.size());

  }
