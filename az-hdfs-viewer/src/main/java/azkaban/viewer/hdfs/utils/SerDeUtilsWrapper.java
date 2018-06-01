package azkaban.viewer.hdfs.utils;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * Adapted from @see org.apache.hadoop.hive.serde2.SerDeUtils to escape binary
 * string and have a valid JSON String
 */
public final class SerDeUtilsWrapper {

    /**
     * Get serialized json using an orc object and corresponding object
     * inspector Adapted from
     * {@link org.apache.hadoop.hive.serde2.SerDeUtils#getJSONString(Object, ObjectInspector)}
     *
     * @param obj
     * @param objIns
     * @return
     */
    public static String getJSON(Object obj, ObjectInspector objIns) {
        StringBuilder sb = new StringBuilder();
        buildJSONString(sb, obj, objIns);
        return sb.toString();
    }

    private static void buildJSONString(StringBuilder sb, Object obj,
        ObjectInspector objIns) {
        String nullStr = "null";
        switch (objIns.getCategory()) {
        case PRIMITIVE: {
            PrimitiveObjectInspector poi = (PrimitiveObjectInspector) objIns;
            if (obj == null) {
                sb.append(nullStr);
            } else {
                switch (poi.getPrimitiveCategory()) {
                case BOOLEAN: {
                    boolean b = ((BooleanObjectInspector) poi).get(obj);
                    sb.append(b ? "true" : "false");
                    break;
                }
                case BYTE: {
                    sb.append(((ByteObjectInspector) poi).get(obj));
                    break;
                }
                case SHORT: {
                    sb.append(((ShortObjectInspector) poi).get(obj));
                    break;
                }
                case INT: {
                    sb.append(((IntObjectInspector) poi).get(obj));
                    break;
                }
                case LONG: {
                    sb.append(((LongObjectInspector) poi).get(obj));
                    break;
                }
                case FLOAT: {
                    sb.append(((FloatObjectInspector) poi).get(obj));
                    break;
                }
                case DOUBLE: {
                    sb.append(((DoubleObjectInspector) poi).get(obj));
                    break;
                }
                case STRING: {
                    sb.append('"');
                    sb.append(SerDeUtils
                        .escapeString(((StringObjectInspector) poi)
                            .getPrimitiveJavaObject(obj)));
                    sb.append('"');
                    break;
                }
                case VARCHAR: {
                    sb.append('"');
                    sb.append(SerDeUtils
                        .escapeString(((HiveVarcharObjectInspector) poi)
                            .getPrimitiveJavaObject(obj).toString()));
                    sb.append('"');
                    break;
                }
                case DATE: {
                    sb.append('"');
                    sb.append(((DateObjectInspector) poi)
                        .getPrimitiveWritableObject(obj));
                    sb.append('"');
                    break;
                }
                case TIMESTAMP: {
                    sb.append('"');
                    sb.append(((TimestampObjectInspector) poi)
                        .getPrimitiveWritableObject(obj));
                    sb.append('"');
                    break;
                }
                case BINARY: {
                    BytesWritable bw =
                        ((BinaryObjectInspector) objIns)
                            .getPrimitiveWritableObject(obj);
                    Text txt = new Text();
                    txt.set(bw.getBytes(), 0, bw.getLength());
                    // Fix to serialize binary type
                    sb.append('"');
                    sb.append(StringEscapeUtils.escapeJava(txt.toString()));
                    sb.append('"');
                    break;
                }
                case DECIMAL: {
                    sb.append(((HiveDecimalObjectInspector) objIns)
                        .getPrimitiveJavaObject(obj));
                    break;
                }
                default:
                    throw new RuntimeException("Unknown primitive type: "
                        + poi.getPrimitiveCategory());
                }
            }
            break;
        }
        case LIST: {
            ListObjectInspector loi = (ListObjectInspector) objIns;
            ObjectInspector listElementObjectInspector =
                loi.getListElementObjectInspector();
            List<?> olist = loi.getList(obj);
            if (olist == null) {
                sb.append(nullStr);
            } else {
                sb.append(SerDeUtils.LBRACKET);
                for (int i = 0; i < olist.size(); i++) {
                    if (i > 0) {
                        sb.append(SerDeUtils.COMMA);
                    }
                    buildJSONString(sb, olist.get(i),
                        listElementObjectInspector);
                }
                sb.append(SerDeUtils.RBRACKET);
            }
            break;
        }
        case MAP: {
            MapObjectInspector moi = (MapObjectInspector) objIns;
            ObjectInspector mapKeyObjectInspector =
                moi.getMapKeyObjectInspector();
            ObjectInspector mapValueObjectInspector =
                moi.getMapValueObjectInspector();
            Map<?, ?> omap = moi.getMap(obj);
            if (omap == null) {
                sb.append(nullStr);
            } else {
                sb.append(SerDeUtils.LBRACE);
                boolean first = true;
                for (Object entry : omap.entrySet()) {
                    if (first) {
                        first = false;
                    } else {
                        sb.append(SerDeUtils.COMMA);
                    }
                    Map.Entry<?, ?> e = (Map.Entry<?, ?>) entry;
                    buildJSONString(sb, e.getKey(), mapKeyObjectInspector);
                    sb.append(SerDeUtils.COLON);
                    buildJSONString(sb, e.getValue(), mapValueObjectInspector);
                }
                sb.append(SerDeUtils.RBRACE);
            }
            break;
        }
        case STRUCT: {
            StructObjectInspector soi = (StructObjectInspector) objIns;
            List<? extends StructField> structFields =
                soi.getAllStructFieldRefs();
            if (obj == null) {
                sb.append(nullStr);
            } else {
                sb.append(SerDeUtils.LBRACE);
                for (int i = 0; i < structFields.size(); i++) {
                    if (i > 0) {
                        sb.append(SerDeUtils.COMMA);
                    }
                    sb.append(SerDeUtils.QUOTE);
                    sb.append(structFields.get(i).getFieldName());
                    sb.append(SerDeUtils.QUOTE);
                    sb.append(SerDeUtils.COLON);
                    buildJSONString(sb,
                        soi.getStructFieldData(obj, structFields.get(i)),
                        structFields.get(i).getFieldObjectInspector());
                }
                sb.append(SerDeUtils.RBRACE);
            }
            break;
        }
        case UNION: {
            UnionObjectInspector uoi = (UnionObjectInspector) objIns;
            if (obj == null) {
                sb.append(nullStr);
            } else {
                sb.append(SerDeUtils.LBRACE);
                sb.append(uoi.getTag(obj));
                sb.append(SerDeUtils.COLON);
                buildJSONString(sb, uoi.getField(obj), uoi
                    .getObjectInspectors().get(uoi.getTag(obj)));
                sb.append(SerDeUtils.RBRACE);
            }
            break;
        }
        default:
            throw new RuntimeException("Unknown type in ObjectInspector!");
        }
    }

}
