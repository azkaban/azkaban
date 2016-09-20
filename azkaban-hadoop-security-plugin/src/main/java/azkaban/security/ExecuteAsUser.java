package azkaban.security;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 *
 * Created by spyne on 9/20/16.
 */
public class ExecuteAsUser {
  private final static Logger log = Logger.getLogger(ExecuteAsUser.class);

  private final String path;

  public ExecuteAsUser(final String path) {
    validate(path);
    this.path = path;
  }

  private void validate(String path) {
    File executeAsUserBinary = new File(path);
    if (!executeAsUserBinary.canExecute()) {
      throw new RuntimeException("Unable to execute execute-as-user binary. Invalid Path: " + path);
    }
  }

  public int execute(final String user, final String command) throws IOException {
    log.info("Command: " + command);
    Process process = new ProcessBuilder()
        .command(partitionCommandLine(constructExecuteAsCommand(user, command)))
        .inheritIO()
        .start();

    int exitCode;
    try {
      exitCode = process.waitFor();
    } catch (InterruptedException e) {
      e.printStackTrace();
      exitCode = 1;
    }
    return exitCode;
  }

  private String constructExecuteAsCommand(String user, String command) {
    return String.format("%s %s %s", path, user, command);
  }

  /**
   * TODO Refactor: function copied from azkaban.jobExecutor.ProcessJob#partitionCommandLine(java.lang.String)
   *
   * Need to be refactored to use just one method. Copy was created to avoid introducing dependencies to
   * azkaban-common
   *
   * Splits the command into a unix like command line structure. Quotes and
   * single quotes are treated as nested strings.
   *
   * @param command
   * @return
   */
  public static String[] partitionCommandLine(final String command) {
    ArrayList<String> commands = new ArrayList<String>();

    int index = 0;

    StringBuffer buffer = new StringBuffer(command.length());

    boolean isApos = false;
    boolean isQuote = false;
    while (index < command.length()) {
      char c = command.charAt(index);

      switch (c) {
        case ' ':
          if (!isQuote && !isApos) {
            String arg = buffer.toString();
            buffer = new StringBuffer(command.length() - index);
            if (arg.length() > 0) {
              commands.add(arg);
            }
          } else {
            buffer.append(c);
          }
          break;
        case '\'':
          if (!isQuote) {
            isApos = !isApos;
          } else {
            buffer.append(c);
          }
          break;
        case '"':
          if (!isApos) {
            isQuote = !isQuote;
          } else {
            buffer.append(c);
          }
          break;
        default:
          buffer.append(c);
      }

      index++;
    }

    if (buffer.length() > 0) {
      String arg = buffer.toString();
      commands.add(arg);
    }

    return commands.toArray(new String[commands.size()]);
  }

}
