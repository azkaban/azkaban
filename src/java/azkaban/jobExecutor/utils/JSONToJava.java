/*
 * Copyright 2010 LinkedIn, Inc
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

package azkaban.jobExecutor.utils;

import com.google.common.base.Function;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

/**
 *
 */
public class JSONToJava implements Function<JSONObject, Map<String, Object>>
{
    @Override
    public Map<String, Object> apply(JSONObject jsonObject)
    {
        Map<String, Object> retVal = new HashMap<String, Object>();

        Iterator keyIterator = jsonObject.keys();
        while (keyIterator.hasNext()) {
            String key = keyIterator.next().toString();

            try {
                retVal.put(key, dispatchCorrectly(jsonObject.get(key)));
            }
            catch (JSONException e) {
                throw new RuntimeException(e);
            }
        }

        return retVal;
    }

    public List<Object> apply(JSONArray jsonArray)
    {
        List<Object> retVal = new ArrayList<Object>(jsonArray.length());

        for (int i = 0; i < jsonArray.length(); ++i) {
            try {
                retVal.add(dispatchCorrectly(jsonArray.get(i)));
            }
            catch (JSONException e) {
                throw new RuntimeException(e);
            }
        }

        return retVal;
    }

    public Object dispatchCorrectly(Object o)
    {
        if (o instanceof JSONObject) {
            return apply((JSONObject) o);
        }
        else if (o instanceof JSONArray) {
            return apply((JSONArray) o);
        }
        else {
            return o.toString();
        }
    }
}
