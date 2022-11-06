package com.guliqi.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;

import java.util.ArrayList;
import java.util.List;

public class PrioritySelector extends GenericUDF {
    private transient ListObjectInspector highPriorityListInspector;
    private transient ListObjectInspector lowPriorityListInspector;
    private transient ListObjectInspector selectorListInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 3) {
            throw new UDFArgumentLengthException("PrioritySelector takes 3 arguments!");
        }
        for (ObjectInspector objectInspector: objectInspectors) {
            if (!(objectInspector instanceof ListObjectInspector)) {
                throw new UDFArgumentException("PrioritySelector arguments must be array!");
            }
        }

        if (!(((ListObjectInspector) objectInspectors[2]).getListElementObjectInspector() instanceof BooleanObjectInspector)) {
            throw new UDFArgumentException("The third parameter must be boolean array!");
        }
        highPriorityListInspector = (ListObjectInspector) objectInspectors[0];
        lowPriorityListInspector = (ListObjectInspector) objectInspectors[1];
        selectorListInspector = (ListObjectInspector) objectInspectors[2];

        return ObjectInspectorFactory.getStandardListObjectInspector(((ListObjectInspector) objectInspectors[0]).getListElementObjectInspector());
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        List<?> highPriorityList = highPriorityListInspector.getList(deferredObjects[0].get());
        List<?> lowPriorityList = lowPriorityListInspector.getList(deferredObjects[1].get());
        List<?> selectorList = selectorListInspector.getList(deferredObjects[2].get());
        List<Object> resultList = new ArrayList<>();
        for (int i = 0; i < selectorList.size(); ++i) {
            if ((Boolean) selectorList.get(i)) {
                resultList.add(highPriorityList.get(i));
            }
            else {
                resultList.add(lowPriorityList.get(i));
            }
        }
        return resultList;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "value in column " + strings[0];
    }
}
