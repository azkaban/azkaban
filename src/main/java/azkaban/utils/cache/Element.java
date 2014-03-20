/*
 * Copyright 2012 LinkedIn Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
