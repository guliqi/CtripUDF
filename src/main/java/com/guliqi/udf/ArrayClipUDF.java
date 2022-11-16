package com.guliqi.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.util.ArrayList;
import java.util.List;


public class ArrayClipUDF extends GenericUDF {
    private transient ListObjectInspector listObjectInspector;
    private transient PrimitiveObjectInspector listElementObjectInspector;
    private transient PrimitiveObjectInspector minValObjectInspector;
    private transient PrimitiveObjectInspector maxValObjectInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 3) {
            throw new UDFArgumentLengthException("array_clip(array, min_val, max_val)");
        }
        if (!(objectInspectors[0] instanceof ListObjectInspector)) {
            throw new UDFArgumentException("array_clip(array, min_val, max_val)");
        }
        listObjectInspector = (ListObjectInspector) objectInspectors[0];
        minValObjectInspector = (PrimitiveObjectInspector) objectInspectors[1];
        maxValObjectInspector = (PrimitiveObjectInspector) objectInspectors[2];

        listElementObjectInspector = (PrimitiveObjectInspector) listObjectInspector.getListElementObjectInspector();
        if ((!NumericUtil.isNumericCategory(listElementObjectInspector.getPrimitiveCategory())) ||
                (!NumericUtil.isNumericCategory(minValObjectInspector.getPrimitiveCategory())) ||
                (!NumericUtil.isNumericCategory(maxValObjectInspector.getPrimitiveCategory()))) {
            throw new UDFArgumentException("array_clip takes numeric inputs.");
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(listObjectInspector.getListElementObjectInspector());
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        @SuppressWarnings("unchecked") List<Number> list = (List<Number>) listObjectInspector.getList(deferredObjects[0].get());
        double minVal = NumericUtil.getNumericValue(minValObjectInspector, deferredObjects[1].get());
        double maxVal = NumericUtil.getNumericValue(maxValObjectInspector, deferredObjects[2].get());
        Number minValNum = (Number) minValObjectInspector.getPrimitiveJavaObject(deferredObjects[1].get());
        Number maxValNum = (Number) minValObjectInspector.getPrimitiveJavaObject(deferredObjects[2].get());
        for (int i = 0; i < list.size(); ++i) {
            double val = NumericUtil.getNumericValue(listElementObjectInspector, list.get(i));
            if (val < minVal) {
                list.set(i, minValNum);
            } else if (val > maxVal) {
                list.set(i, maxValNum);
            }
        }
        return list;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "value in column " + strings[0];
    }
}
