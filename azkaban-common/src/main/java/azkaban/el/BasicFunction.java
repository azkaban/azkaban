/*
 * Copyright 2017 LinkedIn Corp.
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
package azkaban.el;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class BasicFunction {


    private static final String pattern1 = "yyyyMMdd";
    private static final String pattern2 = "yyyy-MM-dd";
    private static SimpleDateFormat date1Format = new SimpleDateFormat(pattern1);
    private static SimpleDateFormat date2Format =new SimpleDateFormat(pattern2);

    public static Long timestamp(){
        Date date = new Date();
        return date.getTime();
    }

    public static int year(){
        Calendar calendar = Calendar.getInstance();
        return calendar.get(Calendar.YEAR);
    }

    public static int month(){
        Calendar calendar = Calendar.getInstance();
        return calendar.get(Calendar.MONTH)+1;
    }

    public static int day(){
        Calendar calendar = Calendar.getInstance();
        return calendar.get(Calendar.DAY_OF_MONTH);
    }

    public static String dateFormat(){
        return dateFormat(pattern1);
    }

    public static String dateFormat(String pattern){

        Date date = new Date();
        String result = null;
        if(pattern.equals(pattern1)){
            result = date1Format.format(date);

        }else if(pattern.equals(pattern2)){
            result = date2Format.format(date);
        }else{
            SimpleDateFormat sdf=new SimpleDateFormat(pattern);
            result = sdf.format(date);
        }
        return result;
    }
}
