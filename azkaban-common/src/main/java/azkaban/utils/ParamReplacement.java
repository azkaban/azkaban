package azkaban.utils;

import com.google.common.collect.Lists;

import java.text.SimpleDateFormat;
import java.util.Date;
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
      // time param, "${time:yyy-MM-dd hh:mm:ss}" will be replaced into like
      // "2015-09-16 11:23:45"
      new Param() {
        public String name() {
          return "time";
        }

        public String replacement(String param) {
          if (param == null) {
            param = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
          }
          SimpleDateFormat format = new SimpleDateFormat(param);
          return format.format(new Date());
        }
      });

  public static String replaceParams(String content) {
    for (Param param : params) {
      content = param.replace(content);
    }
    return content;
  }

}
