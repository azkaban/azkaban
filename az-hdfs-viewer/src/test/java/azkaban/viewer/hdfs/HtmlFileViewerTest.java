package azkaban.viewer.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AccessControlException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * <pre>
 * Test cases for HtmlFileViewer
 *
 * Validate accepted capabilities for the viewer and ensures that it
 * generates the correct content and content type.
 * </pre>
 */
public class HtmlFileViewerTest {
    private static final String EMPTY_HTM = "TestHtmEmptyFile.htm";
    private static final String VALID_HTML = "TestHtmlFile.html";
    FileSystem fs;

    HtmlFileViewer viewer;

    @Before
    public void setUp() throws IOException {
        fs = new LocalFileSystem();
        fs.initialize(fs.getWorkingDirectory().toUri(), new Configuration());
        viewer = new HtmlFileViewer();
    }

    @After
    public void tearDown() throws IOException {
        fs.close();
    }

    @Test
    public void testCapabilities() throws AccessControlException {
        Set<Capability> capabilities = viewer.getCapabilities(fs, getResourcePath(EMPTY_HTM));
        // READ should be the the one and only capability
        assertTrue(capabilities.contains(Capability.READ));
        assertEquals(capabilities.size(), 1);

        capabilities = viewer.getCapabilities(fs, getResourcePath(VALID_HTML));
        // READ should be the the one and only capability
        assertTrue(capabilities.contains(Capability.READ));
        assertEquals(capabilities.size(), 1);
    }

    @Test
    public void testContentType() {
        assertEquals(ContentType.HTML, viewer.getContentType());
    }

    @Test
    public void testEmptyFile() throws IOException {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        viewer.displayFile(fs, getResourcePath(EMPTY_HTM), outStream, 1, 2);
        String output = outStream.toString();
        assertTrue(output.isEmpty());
    }

    @Test
    public void testValidHtmlFile() throws IOException {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        viewer.displayFile(fs, getResourcePath(VALID_HTML), outStream, 1, 2);
        String output = new String(outStream.toByteArray());
        assertEquals(output, "<p>file content</p>\n");
    }

    /* Get Path to a file from resource dir */
    private Path getResourcePath(String filename) {
        URL url =
            Thread.currentThread().getContextClassLoader()
                .getResource("resources/" + filename);
        return new Path(url.getPath());
    }
}
