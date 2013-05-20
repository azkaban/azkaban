package azkaban.utils.cache;

public class Element<T> {
	private Object key;
	private T element;
	private long creationTime = 0;
	private long lastAccessTime = 0;

	public Element(Object key, T element) {
		this.key = key;
		creationTime = System.currentTimeMillis();
		lastAccessTime = creationTime;
		this.element = element;
	}

	public Object getKey() {
		return key;
	}

	public T getElement() {
		lastAccessTime = System.currentTimeMillis();
		return element;
	}

	public long getCreationTime() {
		return creationTime;
	}

	public long getLastUpdateTime() {
		return lastAccessTime;
	}
}