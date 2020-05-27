package azkaban.executor;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExecutorTest {
  @Test
  public void testObjectIdentity() {
    final Executor e = new Executor(0, "example.com", 12345, false);
    final Executor e2 = new Executor(0, "example.com", 12345, false);

    assertEquals("Executor identity must compare instance fields", e, e2);

    // Give the HashSet some room to discourage collisions
    final Set<Executor> set = new HashSet<>(1024);
    set.add(e);

    assertTrue("Set must Executor we just added", set.contains(e));

    e.setActive(true);

    assertTrue("Set must still contain the Executor after mutating it", set.contains(e));
    assertEquals("Executor identity must not depend on mutable fields", e, e2);
  }
}
