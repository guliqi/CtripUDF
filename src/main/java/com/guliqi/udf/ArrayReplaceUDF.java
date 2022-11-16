package com.guliqi.udf;

import org.apache.arrow.flatbuf.Int;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class ArrayReplaceUDF extends GenericUDF {
    private transient ListObjectInspector listObjectInspector;
    private transient PrimitiveObjectInspector listElementObjectInspector;
    private transient PrimitiveObjectInspector oldValObjectInspector;
    private transient PrimitiveObjectInspector newValObjectInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 3) {
            throw new UDFArgumentLengthException("array_replace(array, old_val, new_val)");
        }
        if (!(objectInspectors[0] instanceof ListObjectInspector)) {
            throw new UDFArgumentException("array_replace(array, old_val, new_val)");
        }
        listObjectInspector = (ListObjectInspector) objectInspectors[0];
        listElementObjectInspector = (PrimitiveObjectInspector) listObjectInspector.getListElementObjectInspector();
        oldValObjectInspector = (PrimitiveObjectInspector) objectInspectors[1];
        newValObjectInspector = (PrimitiveObjectInspector) objectInspectors[2];

        if (listElementObjectInspector.getPrimitiveCategory() != oldValObjectInspector.getPrimitiveCategory() ||
                listElementObjectInspector.getPrimitiveCategory() != newValObjectInspector.getPrimitiveCategory()) {
            throw new UDFArgumentException("arguments must be the same primitive type.");
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(listObjectInspector.getListElementObjectInspector());
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        List<?> list = listObjectInspector.getList(deferredObjects[0].get());
        Object oldVal = oldValObjectInspector.getPrimitiveJavaObject(deferredObjects[1].get());
        Object newVal = newValObjectInspector.getPrimitiveJavaObject(deferredObjects[2].get());
        List<Object> result = new ArrayList<>();

        for (Object obj : list) {
            if (obj.equals(oldVal)) {
                result.add(newVal);
            } else {
                result.add(obj);
            }
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "value in column " + strings[0];
    }
}
