package azkaban.filter;

/**
 * This lists the methods required to build a filter.
 *
 * @param <T>: the type of the objects to be compared.
 * @param <V>: the type of the object to be used for filtering.
 */
public interface Filter<T, V> {

  /**
   * function to analyze the target item according to the reference object to decide whether the
   * item should be filtered.
   *
   * @param filteringTarget   object to be checked.
   * @param referencingObject object which contains statistics based on which a decision is made
   *                          whether the object being checked need to be filtered or not.
   * @return true if the check passed, false if check failed, which means the item need to be
   * filtered.
   */
   boolean filterTarget(final T filteringTarget, final V referencingObject);


}
