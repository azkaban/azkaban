package azkaban.viewer.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import azkaban.viewer.hdfs.utils.SerDeUtilsWrapper;

/**
 * This class implements a viewer for ORC files
 *
 * @author gaggarwa
 */
public class ORCFileViewer extends HdfsFileViewer {
    private static Logger logger = Logger.getLogger(ORCFileViewer.class);
    // Will spend 5 seconds trying to pull data and then stop.
    private final static long STOP_TIME = 5000l;
    private final static int JSON_INDENT = 2;

    private static final String VIEWER_NAME = "ORC";

    @Override
    public String getName() {
        return VIEWER_NAME;
    }

    /**
     * Get ORCFileViewer functionalities. Currently schema and read are
     * supported. {@inheritDoc}
     *
     * @see HdfsFileViewer#getCapabilities(org.apache.hadoop.fs.FileSystem,
     *      org.apache.hadoop.fs.Path)
     */
    @Override
    public Set<Capability> getCapabilities(FileSystem fs, Path path) {
        if (logger.isDebugEnabled()) {
            logger.debug("orc file path: " + path.toUri().getPath());
        }

        Reader orcReader = null;
        RecordReader recordReader = null;
        try {
            // no need to close orcreader
            orcReader = OrcFile.createReader(fs, path);
            recordReader = orcReader.rows(null);
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug(path.toUri().getPath() + " is not a ORC file.");
                logger.debug("Error in opening ORC file: "
                    + e.getLocalizedMessage());
            }
            return EnumSet.noneOf(Capability.class);
        } finally {
            if (recordReader != null) {
                try {
                    recordReader.close();
                } catch (IOException e) {
                    logger.error(e);
                }
            }
        }
        return EnumSet.of(Capability.READ, Capability.SCHEMA);
    }

    /**
     * Reads orc file and write to outputstream in json format {@inheritDoc}
     *
     * @throws IOException
     * @throws
     *
     * @see HdfsFileViewer#displayFile(org.apache.hadoop.fs.FileSystem,
     *      org.apache.hadoop.fs.Path, OutputStream, int, int)
     */
    @Override
    public void displayFile(FileSystem fs, Path path, OutputStream outStream,
        int startLine, int endLine) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("displaying orc file:" + path.toUri().getPath());
        }
        StringBuilder ret = new StringBuilder();
        Reader orcreader = null;
        RecordReader reader = null;
        Object row = null;
        try {
            int lineNum = 1;

            orcreader = OrcFile.createReader(fs, path);
            reader = orcreader.rows(null);
            long endTime = System.currentTimeMillis() + STOP_TIME;
            while (reader.hasNext() && lineNum <= endLine
                && System.currentTimeMillis() <= endTime) {
                row = reader.next(row);
                if (lineNum >= startLine) {
                    ret.append(String.format("Record %d:\n", lineNum));
                    String jsonString =
                        SerDeUtilsWrapper.getJSON(row,
                            orcreader.getObjectInspector());
                    try {
                        JSONObject jsonobj = new JSONObject(jsonString);
                        ret.append(jsonobj.toString(JSON_INDENT));
                    } catch (JSONException e) {
                        logger.error("Failed to parse json as JSONObject", e);
                        // default to unformatted json string
                        ret.append(jsonString);
                    }
                    ret.append("\n\n");
                }
                lineNum++;
            }
            outStream.write(ret.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            outStream.write(("Error in display orc file: " + e
                .getLocalizedMessage()).getBytes("UTF-8"));
            throw e;
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    /**
     * Get schema in same syntax as in hadoop --orcdump {@inheritDoc}
     *
     * @see HdfsFileViewer#getSchema(org.apache.hadoop.fs.FileSystem,
     *      org.apache.hadoop.fs.Path)
     */
    @Override
    public String getSchema(FileSystem fs, Path path) {
        String schema = null;
        try {
            Reader orcReader = OrcFile.createReader(fs, path);
            schema = orcReader.getObjectInspector().getTypeName();
        } catch (IOException e) {
            logger
                .warn("Cannot get schema for file: " + path.toUri().getPath());
            return null;
        }

        return schema;
    }

}
