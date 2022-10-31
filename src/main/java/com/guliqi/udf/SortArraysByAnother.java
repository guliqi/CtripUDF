package com.guliqi.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class SortArraysByAnother extends GenericUDF {
    private transient ObjectInspector[] inputInspectors;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length < 2) {
            throw new UDFArgumentLengthException("SortArraysByAnother takes as least 2 arguments!");
        }
        for (ObjectInspector objectInspector: objectInspectors) {
            if (!(objectInspector instanceof ListObjectInspector)) {
                throw new UDFArgumentException("SortArraysByAnother arguments must be array!");
            }
        }
        inputInspectors = objectInspectors;
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        for (int i = 0; i < objectInspectors.length; i++) {
            fieldNames.add("array" + i);
            fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
                    ((ListObjectInspector) objectInspectors[i]).getListElementObjectInspector()));
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        ListObjectInspector baseListInspector = (ListObjectInspector) inputInspectors[0];
        List baseList = baseListInspector.getList(deferredObjects[0].get());
        final ObjectInspector valInspector = baseListInspector.getListElementObjectInspector();
        final Integer[] sortIndex = IntStream.range(0, baseList.size()).boxed().toArray(Integer[]::new);
        Arrays.sort(sortIndex, (o1, o2) -> ObjectInspectorUtils.compare(baseList.get(o1), valInspector, baseList.get(o2), valInspector));

        ListObjectInspector sortListOI;
        List sortList;
        List retList;
        Object[] result = new Object[deferredObjects.length];
        for (int i = 0; i < deferredObjects.length; i++) {
             sortListOI = (ListObjectInspector) inputInspectors[i];
             sortList = sortListOI.getList(deferredObjects[i].get());
             retList = new ArrayList<>();
             for (int index: sortIndex) {
                 retList.add(sortList.get(index));
             }
            result[i] = retList;
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "value in column " + strings[0];
    }
}
