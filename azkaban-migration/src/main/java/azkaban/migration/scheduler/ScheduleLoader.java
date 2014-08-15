/*
 * Copyright 2012 LinkedIn, Inc
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

package azkaban.migration.scheduler;

import java.util.List;

@Deprecated
public interface ScheduleLoader {

  public void insertSchedule(Schedule s) throws ScheduleManagerException;

  public void updateSchedule(Schedule s) throws ScheduleManagerException;

  public List<Schedule> loadSchedules() throws ScheduleManagerException;

  public void removeSchedule(Schedule s) throws ScheduleManagerException;

  public void updateNextExecTime(Schedule s) throws ScheduleManagerException;

}
