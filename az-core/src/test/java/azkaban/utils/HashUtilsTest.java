package azkaban.utils;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

import java.io.File;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class HashUtilsTest {
  @Rule
  public final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private static final String SAMPLE_STR = "abcd123";

  private static final String SAMPLE_STR_MD5 = "79CFEB94595DE33B3326C06AB1C7DBDA";
  private static final byte[] SAMPLE_STR_MD5_BYTES =
      new byte[]{121, -49, -21, -108, 89, 93, -29, 59, 51, 38, -64, 106, -79, -57, -37, -38};

  private static final String SAMPLE_STR_SHA1 = "7C3607B8E61BCF1944E9E8503A660F21F4B6F3F1";
  private static final byte[] SAMPLE_STR_SHA1_BYTES =
      new byte[]{124, 54, 7, -72, -26, 27, -49, 25, 68, -23, -24, 80, 58, 102, 15, 33, -12, -74, -13, -15};

  private File sampleFile;

  @Before
  public void setup() throws Exception {
    sampleFile = TEMP_DIR.newFile("blahblah.jar");
    FileUtils.writeStringToFile(sampleFile, SAMPLE_STR);
  }

  @Test
  public void MD5BytesFromFile() throws Exception {
    assertThat(HashUtils.MD5.getHashBytes(sampleFile), is(SAMPLE_STR_MD5_BYTES));
  }

  @Test
  public void SHA1BytesFromFile() throws Exception {
    assertThat(HashUtils.SHA1.getHashBytes(sampleFile), is(SAMPLE_STR_SHA1_BYTES));
  }

  @Test
  public void MD5BytesFromString() throws Exception {
    assertThat(HashUtils.MD5.getHashBytes(SAMPLE_STR), is(SAMPLE_STR_MD5_BYTES));
  }

  @Test
  public void SHA1BytesFromString() throws Exception {
    assertThat(HashUtils.SHA1.getHashBytes(SAMPLE_STR), is(SAMPLE_STR_SHA1_BYTES));
  }

  @Test
  public void SHA1StringFromFile() throws Exception {
    assertEquals(SAMPLE_STR_SHA1.toLowerCase(), HashUtils.SHA1.getHashStr(sampleFile));
  }

  @Test
  public void SHA1StringFromString() throws Exception {
    assertEquals(SAMPLE_STR_SHA1.toLowerCase(), HashUtils.SHA1.getHashStr(SAMPLE_STR));
  }

  @Test
  public void isSameHash() throws Exception {
    assertTrue(HashUtils.isSameHash(SAMPLE_STR_MD5, SAMPLE_STR_MD5_BYTES));
    assertTrue(HashUtils.isSameHash(SAMPLE_STR_SHA1, SAMPLE_STR_SHA1_BYTES));
  }

  @Test
  public void stringToBytesToString() throws Exception {
    assertEquals(SAMPLE_STR_MD5.toLowerCase(),
        HashUtils.bytesHashToString(HashUtils.stringHashToBytes(SAMPLE_STR_MD5)));
  }

  @Test
  public void sanitizeValidMD5() throws Exception {
    assertEquals(SAMPLE_STR_MD5.toLowerCase(), HashUtils.MD5.sanitizeHashStr(SAMPLE_STR_MD5));
  }

  @Test
  public void sanitizeValidSHA1() throws Exception {
    assertEquals(SAMPLE_STR_SHA1.toLowerCase(), HashUtils.SHA1.sanitizeHashStr(SAMPLE_STR_SHA1));
  }

  @Test(expected = InvalidHashException.class)
  public void sanitizeInvalidCharacterHash() throws Exception {
    String md5WithInvalidFirstCharacter = "'" + SAMPLE_STR_MD5.substring(1);
    HashUtils.MD5.sanitizeHashStr(md5WithInvalidFirstCharacter);
  }

  @Test(expected = InvalidHashException.class)
  public void sanitizeTooLongMD5() throws Exception {
    HashUtils.MD5.sanitizeHashStr(SAMPLE_STR_MD5 + "A");
  }

  @Test(expected = InvalidHashException.class)
  public void sanitizeTooLongSHA1() throws Exception {
    HashUtils.SHA1.sanitizeHashStr(SAMPLE_STR_SHA1 + "A");
  }
}
