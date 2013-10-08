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

package azkaban.sla;

import java.util.List;

public interface SLALoader {

	public void insertSLA(SLA s) throws SLAManagerException;
	
	public List<SLA> loadSLAs() throws SLAManagerException;
	
	public void removeSLA(SLA s) throws SLAManagerException;

//	public void updateSLA(SLA s) throws SLAManagerException;
}
