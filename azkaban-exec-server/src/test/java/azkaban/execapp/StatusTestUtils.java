package azkaban.execapp;

import azkaban.executor.ExecutableNode;
import azkaban.executor.Status;

public class StatusTestUtils {

  public static void waitForStatus(final ExecutableNode node, final Status status) {
    for (int i = 0; i < 1000; i++) {
      if (node.getStatus() == status) {
        break;
      }
      synchronized (EventCollectorListener.handleEvent) {
        try {
          EventCollectorListener.handleEvent.wait(10L);
        } catch (final InterruptedException e) {
          i--;
        }
      }
    }
  }

}
