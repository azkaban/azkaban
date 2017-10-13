package azkaban.execapp.event;

import azkaban.execapp.FlowRunner;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.Status;
import org.junit.Assert;

public class FlowWatcherTestUtil {

  public static void assertPipelineLevel1(final FlowRunner runner1,
      final FlowRunner runner2) throws Exception {

    run(runner1, runner2);

    final ExecutableFlow first = runner1.getExecutableFlow();
    final ExecutableFlow second = runner2.getExecutableFlow();

    for (final ExecutableNode node : second.getExecutableNodes()) {
      Assert.assertEquals(Status.SUCCEEDED, node.getStatus());

      // check it's start time is after the first's children.
      final ExecutableNode watchedNode = first.getExecutableNode(node.getId());
      if (watchedNode == null) {
        continue;
      }
      Assert.assertEquals(Status.SUCCEEDED, watchedNode.getStatus());

      System.out.println("Node " + node.getId() + " start: "
          + node.getStartTime() + " dependent on " + watchedNode.getId() + " "
          + watchedNode.getEndTime() + " diff: "
          + (node.getStartTime() - watchedNode.getEndTime()));

      Assert.assertTrue(node.getStartTime() >= watchedNode.getEndTime());

      long minParentDiff = 0;
      if (node.getInNodes().size() > 0) {
        minParentDiff = Long.MAX_VALUE;
        for (final String dependency : node.getInNodes()) {
          final ExecutableNode parent = second.getExecutableNode(dependency);
          final long diff = node.getStartTime() - parent.getEndTime();
          minParentDiff = Math.min(minParentDiff, diff);
        }
      }
      final long diff = node.getStartTime() - watchedNode.getEndTime();
      Assert.assertTrue(minParentDiff < 500 || diff < 500);
    }
  }

  public static void assertPipelineLevel2(final FlowRunner runner1, final FlowRunner runner2,
      final boolean job4Skipped) throws Exception {
    run(runner1, runner2);
    assertPipelineLevel2(runner1.getExecutableFlow(), runner2.getExecutableFlow(), job4Skipped);
  }

  public static void assertPipelineLevel2(final ExecutableFlow first,
      final ExecutableFlow second, final boolean job4Skipped) {
    for (final ExecutableNode node : second.getExecutableNodes()) {
      Assert.assertEquals(Status.SUCCEEDED, node.getStatus());

      // check it's start time is after the first's children.
      final ExecutableNode watchedNode = first.getExecutableNode(node.getId());
      if (watchedNode == null) {
        continue;
      }
      Assert.assertEquals(watchedNode.getStatus(),
          job4Skipped && watchedNode.getId().equals("job4") ? Status.READY : Status.SUCCEEDED);

      long minDiff = Long.MAX_VALUE;
      for (final String watchedChild : watchedNode.getOutNodes()) {
        final ExecutableNode child = first.getExecutableNode(watchedChild);
        if (child == null) {
          continue;
        }
        Assert.assertEquals(child.getStatus(),
            job4Skipped && child.getId().equals("job4") ? Status.READY : Status.SUCCEEDED);
        final long diff = node.getStartTime() - child.getEndTime();
        minDiff = Math.min(minDiff, diff);
        Assert.assertTrue(
            "Node " + node.getId() + " start: " + node.getStartTime() + " dependent on "
                + watchedChild + " " + child.getEndTime() + " diff: " + diff,
            node.getStartTime() >= child.getEndTime());
      }

      long minParentDiff = Long.MAX_VALUE;
      for (final String dependency : node.getInNodes()) {
        final ExecutableNode parent = second.getExecutableNode(dependency);
        final long diff = node.getStartTime() - parent.getEndTime();
        minParentDiff = Math.min(minParentDiff, diff);
      }
      Assert.assertTrue("minPipelineTimeDiff:" + minDiff
              + " minDependencyTimeDiff:" + minParentDiff,
          minParentDiff < 5000 || minDiff < 5000);
    }
  }

  private static void run(final FlowRunner runner1, final FlowRunner runner2)
      throws InterruptedException {
    final Thread runner1Thread = new Thread(runner1);
    final Thread runner2Thread = new Thread(runner2);
    runner1Thread.start();
    runner2Thread.start();
    runner2Thread.join();
  }
}
