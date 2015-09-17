package azkaban.utils;

import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParamReplacement {
  private static abstract class Param {
    abstract String name();

    abstract String replacement(String param);

    String replace(String content) {
      Pattern p = Pattern.compile("\\$\\{" + name() + "(?:|:.*?)}");
      Matcher m = p.matcher(content);
      String filledContent = "";
      int lastIndex = 0;
      while (m.find()) {
        String g = m.group();
        String replacement;
        if (("${" + name() + "}").equals(g)) {
          replacement = replacement(null);
        } else {
          String pstr =
              org.apache.commons.lang.StringUtils.removeStart(g, "${" + name()
                  + ":");
          pstr = org.apache.commons.lang.StringUtils.removeEnd(pstr, "}");
          replacement = replacement(pstr);
        }
        filledContent += content.substring(lastIndex, m.start()) + replacement;
        lastIndex = m.end();
      }
      filledContent += content.substring(lastIndex);
      return filledContent;
    }
  }

  private static List<Param> params = Lists.<Param> newArrayList(
  // time param, format: "${time:<plus period>,<time format>}", like
  // "${time:-1day,yyyy-MM-dd HH:mm:ss}" will be replaced into like
  // "2015-09-16 11:23:45", if current time is "2015-09-17 11:23:45"
      new Param() {
        PeriodFormatter periodFormatter = new PeriodFormatterBuilder()
            .appendYears().appendSuffix("year").appendMonths()
            .appendSuffix("month").appendDays().appendSuffix("day")
            .appendHours().appendSuffix("hour").appendMinutes()
            .appendSuffix("minute").appendSeconds().appendSuffix("second")
            .toFormatter();

        public String name() {
          return "time";
        }

        public String replacement(String param) {
          if (param == null) {
            param = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
          }
          String[] ps = param.split(",", 2);
          if (ps.length != 2) {
            throw new RuntimeException(
                String
                    .format(
                        "illegal time param format: %s, should be like: ${time:-1day,yyyy-MM-dd HH:mm:ss}",
                        param));
          }
          Period p;
          if (ps[0].trim().equals("")) {
            p = new Period();
          } else {
            p = periodFormatter.parsePeriod(ps[0]);
          }
          return DateTime.now().plus(p).toString(ps[1]);
        }
      });

  public static String replaceParams(String content) {
    for (Param param : params) {
      content = param.replace(content);
    }
    return content;
  }
}
