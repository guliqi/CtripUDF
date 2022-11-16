package com.guliqi.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.util.Collections;
import java.util.List;

@Description(
        name = "array_min",
        value = "_FUNC_(array) - Returns the minimum value in an array.",
        extended = "Example:\n  > SELECT _FUNC_(array(4, 2, 3)) FROM src LIMIT 1;\n  2"
)
public class ArrayMinUDF extends GenericUDF {
    private transient ListObjectInspector listObjectInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1) {
            throw new UDFArgumentLengthException("array_min applies on single array");
        }
        if (!(objectInspectors[0] instanceof ListObjectInspector)) {
            throw new UDFArgumentException("array_min applies on single array");
        }
        listObjectInspector = (ListObjectInspector) objectInspectors[0];
        return listObjectInspector.getListElementObjectInspector();
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        List<?> list = listObjectInspector.getList(deferredObjects[0].get());
        final ObjectInspector elementInspector = listObjectInspector.getListElementObjectInspector();
        return Collections.min(list, ((o1, o2) -> ObjectInspectorUtils.compare(o1, elementInspector, o2, elementInspector)));
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "value in column " + strings[0];
    }
}
