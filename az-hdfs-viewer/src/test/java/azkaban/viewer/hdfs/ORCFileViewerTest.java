package azkaban.viewer.hdfs;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.EnumSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * <pre>
 * Test cases for ORCFileViewer
 *
 * Validate capability for ORC files Validate false capability for avro, text
 * and parquet schema for ORC files verify raw records from orc files
 * </pre>
 */
public class ORCFileViewerTest {
    ORCFileViewer viewer;
    Set<Capability> supportedCapabilities;
    Set<Capability> unSupportedCapabilities;
    FileSystem fs;

    @Before
    public void setUp() throws IOException {
        fs = new LocalFileSystem();
        fs.initialize(fs.getWorkingDirectory().toUri(), new Configuration());
        viewer = new ORCFileViewer();
        supportedCapabilities = EnumSet.of(Capability.READ, Capability.SCHEMA);
        unSupportedCapabilities = EnumSet.noneOf(Capability.class);
    }

    @After
    public void tearDown() throws IOException {
        fs.close();
    }

    /* Calls ORCFileViewer#displayFile and parse results */
    String displayRecordWrapper(String filename, int startRecord, int endRecord)
        throws IOException {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        viewer.displayFile(fs, getResourcePath(filename), outStream,
            startRecord, endRecord);
        String records = new String(outStream.toByteArray());
        records = records.replaceAll("Record [0-9]*:", "");
        records = StringUtils.deleteWhitespace(records);
        return records;
    }

    /* Get Path to a file from resource dir */
    Path getResourcePath(String filename) {
        URL url =
            Thread.currentThread().getContextClassLoader()
                .getResource("resources/" + filename);
        return new Path(url.getPath());
    }

    /* verify capability for empty orc files */
    @Test
    public void orcEmptyFileCapability() throws IOException {
        assertEquals(supportedCapabilities, viewer.getCapabilities(fs,
            getResourcePath("TestOrcFile.emptyFile.orc")));
    }

    /* verify capability for generic orc files */
    @Test
    public void genericORCFileCapability() throws IOException {
        assertEquals(supportedCapabilities, viewer.getCapabilities(fs,
            getResourcePath("TestOrcFile.testPredicatePushdown.orc")));
    }

    /* verify capability for orc files with binary type */
    @Test
    public void binaryTypeORCFileCapability() throws IOException {
        assertEquals(supportedCapabilities, viewer.getCapabilities(fs,
            getResourcePath("TestOrcFile.testStringAndBinaryStatistics.orc")));
    }

    /* verify capability for snappy compressed orc file */
    @Test
    public void snappyCompressedORCFileCapability() throws IOException {
        assertEquals(supportedCapabilities, viewer.getCapabilities(fs,
            getResourcePath("TestOrcFile.testSnappy.orc")));
    }

    /* verify capability for union type orc file */
    @Test
    public void unionTypeORCFileCapability() throws IOException {
        assertEquals(supportedCapabilities, viewer.getCapabilities(fs,
            getResourcePath("TestOrcFile.testUnionAndTimestamp.orc")));
    }

    /* verify capability for avro files */
    @Test
    public void noAvroCapability() throws IOException {
        assertEquals(unSupportedCapabilities,
            viewer.getCapabilities(fs, getResourcePath("TestAvro.avro")));
    }

    /* verify capability for text files */
    @Test
    public void noTextCapability() throws IOException {
        assertEquals(unSupportedCapabilities,
            viewer.getCapabilities(fs, getResourcePath("TestTextFile.txt")));
    }

    /* verify capability for parquet files */
    @Test
    public void noParquetCapability() throws IOException {
        assertEquals(unSupportedCapabilities, viewer.getCapabilities(fs,
            getResourcePath("TestParquetFile.parquet")));
    }

    /* verify schema for empty orc files */
    @Test
    public void emptyORCFileSchema() throws IOException {
        String schema =
            "struct<boolean1:boolean,byte1:tinyint,short1:smallint,int1:int,long1:bigint,"
                + "float1:float,double1:double,bytes1:binary,string1:string,"
                + "middle:struct<list:array<struct<int1:int,string1:string>>>,"
                + "list:array<struct<int1:int,string1:string>>,map:map<string,struct<int1:int,string1:string>>>";
        assertEquals(schema,
            viewer.getSchema(fs, getResourcePath("TestOrcFile.emptyFile.orc")));
    }

    /* verify schema for generic orc files */
    @Test
    public void genericORCFileSchema() throws IOException {
        assertEquals("struct<int1:int,string1:string>", viewer.getSchema(fs,
            getResourcePath("TestOrcFile.testPredicatePushdown.orc")));
    }

    /* verify schema for orc files with binary type */
    @Test
    public void binaryTypeFileSchema() throws IOException {
        assertEquals("struct<bytes1:binary,string1:string>", viewer.getSchema(
            fs,
            getResourcePath("TestOrcFile.testStringAndBinaryStatistics.orc")));
    }

    /* verify schema for snappy compressed orc file */
    @Test
    public void snappyCompressedFileSchema() throws IOException {
        assertEquals("struct<int1:int,string1:string>",
            viewer.getSchema(fs, getResourcePath("TestOrcFile.testSnappy.orc")));
    }

    /* verify schema for union type orc file */
    @Test
    public void unionTypeFileSchema() throws IOException {
        assertEquals(
            "struct<time:timestamp,union:uniontype<int,string>,decimal:decimal>",
            viewer.getSchema(fs,
                getResourcePath("TestOrcFile.testUnionAndTimestamp.orc")));
    }

    /* verify record display for empty orc files */
    @Test
    public void emptyORCFileDisplay() throws IOException {
        String actual = displayRecordWrapper("TestOrcFile.emptyFile.orc", 1, 1);
        assertEquals("", actual);
    }

    /* verify record display for generic orc files */
    @Test
    public void genericORCFileDisplay() throws IOException {
        String actual =
            displayRecordWrapper("TestOrcFile.testPredicatePushdown.orc", 2, 2);
        assertEquals("{\"int1\":300,\"string1\":\"a\"}", actual);
    }

    /* verify record display for orc files with binary type */
    @Test
    public void binaryTypeFileDisplay() throws IOException {
        String actual =
            displayRecordWrapper(
                "TestOrcFile.testStringAndBinaryStatistics.orc", 4, 4);
        assertEquals("{\"bytes1\":null,\"string1\":\"hi\"}", actual);
    }

    /* verify record display for snappy compressed orc file */
    @Test
    public void snappyCompressedFileDisplay() throws IOException {
        String actual =
            displayRecordWrapper("TestOrcFile.testSnappy.orc", 2, 2);
        assertEquals("{\"int1\":1181413113,\"string1\":\"382fdaaa\"}", actual);
    }

    /* verify record display for union type orc file */
    @Test
    public void unionTypeFileDisplay() throws IOException {
        String actual =
            displayRecordWrapper("TestOrcFile.testUnionAndTimestamp.orc", 5, 5);
        assertEquals("{\"decimal\":null,\"time\":null,\"union\":{\"1\":null}}",
            actual);
    }

}
