package azkaban.project.validator;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

/**
 * Test adding messages to {@link ValidationReport}
 */
public class ValidationReportTest {

  @Test
  public void testAddWarnLevelInfoMsg() {
    final ValidationReport report = new ValidationReport();
    final String msg = "test warn level info message.";
    report.addWarnLevelInfoMsg(msg);
    for (final String info : report.getInfoMsgs()) {
      assertEquals("Info message added through addWarnLevelInfoMsg should have level set to WARN",
          ValidationReport.getInfoMsgLevel(info), ValidationStatus.WARN);
      assertEquals("Retrieved info message does not match the original one.",
          ValidationReport.getInfoMsg(info), msg);
    }
  }

  @Test
  public void testAddErrorLevelInfoMsg() {
    final ValidationReport report = new ValidationReport();
    final String msg = "test error level error message.";
    report.addErrorLevelInfoMsg(msg);
    for (final String info : report.getInfoMsgs()) {
      assertEquals("Info message added through addErrorLevelInfoMsg should have level set to ERROR",
          ValidationReport.getInfoMsgLevel(info), ValidationStatus.ERROR);
      assertEquals("Retrieved info message does not match the original one.",
          ValidationReport.getInfoMsg(info), msg);
    }
  }

  @Test
  public void testAddMsgs() {
    final ValidationReport report = new ValidationReport();
    final Set<String> msgs = new HashSet<>();
    msgs.add("test msg 1.");
    msgs.add("test msg 2.");
    report.addWarningMsgs(msgs);
    assertEquals("Level of severity is not warn.",
        report.getStatus(), ValidationStatus.WARN);
    report.addErrorMsgs(msgs);
    assertEquals("Number of error messages retrieved does not match.",
        report.getErrorMsgs().size(), 2);
    assertEquals("Number of warn messages retrieved does not match.",
        report.getWarningMsgs().size(), 2);
    assertEquals("Level of severity is not error.",
        report.getStatus(), ValidationStatus.ERROR);
  }

  @Test
  public void testAddModifiedRemovedFiles() {
    final ValidationReport report = new ValidationReport();
    File a = new File("/a.txt");
    File b = new File("/b.txt");
    File c = new File("/c.txt");
    File d = new File("/d.txt");
    File e = new File("/e.txt");

    final Set<File> removedFiles = new HashSet<>();
    removedFiles.add(a);
    removedFiles.add(b);
    final Set<File> modifiedFiles = new HashSet<>();
    removedFiles.add(c);

    // Exercise both the methods to add individual files, and methods to add collections of files.
    report.addRemovedFile(d);
    report.addModifiedFile(e);
    report.addRemovedFiles(removedFiles);
    report.addModifiedFiles(modifiedFiles);

    final Set<File> expectedRemovedFiles = new HashSet<>();
    expectedRemovedFiles.addAll(removedFiles);
    expectedRemovedFiles.add(d);

    final Set<File> expectedModifiedFiles = new HashSet<>();
    expectedModifiedFiles.addAll(modifiedFiles);
    expectedModifiedFiles.add(e);

    assertEquals(expectedRemovedFiles, report.getRemovedFiles());
    assertEquals(expectedModifiedFiles, report.getModifiedFiles());
  }
}
