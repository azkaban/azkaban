package azkaban.executor;

import azkaban.utils.Props;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * A set of tags (strings) that describe the capabilities of an executor or the requirements of a
 * flow. Flows are only executed by executors that provide (at least) all of their requirements.
 */
public class ExecutorTags implements Iterable<String> {

  private final Set<String> tags;

  public ExecutorTags(final Collection<? extends String> tags) {
    this.tags = Collections.unmodifiableSet(tags == null ? Collections.emptySet() :
        new TreeSet<>(tags));
  }

  public static ExecutorTags getTagsFromProps(final Props props, final String key) {
    return new ExecutorTags(props.getStringList(key));
  }

  public static ExecutorTags empty() {
    return new ExecutorTags(Collections.emptyList());
  }

  @Override
  public Iterator<String> iterator() {
    return this.tags.iterator();
  }
}
