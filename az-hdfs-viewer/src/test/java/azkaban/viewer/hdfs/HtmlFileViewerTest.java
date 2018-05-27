package azkaban.viewer.hdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        this.fs = new LocalFileSystem();
        this.fs.initialize(this.fs.getWorkingDirectory().toUri(), new Configuration());
        this.viewer = new HtmlFileViewer();
    }

    @After
    public void tearDown() throws IOException {
        this.fs.close();
    }

    @Test
    public void testCapabilities() throws AccessControlException {
        Set<Capability> capabilities = this.viewer
            .getCapabilities(this.fs, getResourcePath(EMPTY_HTM));
        // READ should be the the one and only capability
        assertTrue(capabilities.contains(Capability.READ));
        assertEquals(capabilities.size(), 1);

        capabilities = this.viewer.getCapabilities(this.fs, getResourcePath(VALID_HTML));
        // READ should be the the one and only capability
        assertTrue(capabilities.contains(Capability.READ));
        assertEquals(capabilities.size(), 1);
    }

    @Test
    public void testContentType() {
        assertEquals(ContentType.HTML, this.viewer.getContentType());
    }

    @Test
    public void testEmptyFile() throws IOException {
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        this.viewer.displayFile(this.fs, getResourcePath(EMPTY_HTM), outStream, 1, 2);
        final String output = outStream.toString();
        assertTrue(output.isEmpty());
    }

    @Test
    @SuppressWarnings("DefaultCharset")
    public void testValidHtmlFile() throws IOException {
        final ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        this.viewer.displayFile(this.fs, getResourcePath(VALID_HTML), outStream, 1, 2);
        final String output = new String(outStream.toByteArray());
        assertEquals(output, "<p>file content</p>\n");
    }

    /* Get Path to a file from resource dir */
    private Path getResourcePath(final String filename) {
        final URL url =
            Thread.currentThread().getContextClassLoader()
                .getResource("resources/" + filename);
        return new Path(url.getPath());
    }
}
