package azkaban.executor;

import azkaban.utils.TypedMapWrapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExecutorData {

  private final ExecutorTags tags;

  public static ExecutorData fromObject(final Object object) {
    final TypedMapWrapper<String, Object> wrapper = new TypedMapWrapper<>(
        (Map<String, Object>) object);

    final ExecutorTags tags;
    if (wrapper.containsKey("tags")) {
      tags = new ExecutorTags(wrapper.getStringCollection("tags"));
    } else {
      tags = ExecutorTags.empty();
    }

    return new ExecutorData(tags);
  }

  public ExecutorData(final ExecutorTags tags) {
    this.tags = tags;
  }

  public ExecutorTags getTags() {
    return this.tags;
  }

  public Object toObject() {
    final Map<String, Object> obj = new HashMap<>();

    final List<String> tagList = new ArrayList<>();
    getTags().forEach(tagList::add);
    obj.put("tags", tagList);

    return obj;
  }
}
