package com.guliqi.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class RefSort extends GenericUDTF {
    private transient ListObjectInspector[] inputInspectors;

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<? extends StructField> inputsFields = argOIs.getAllStructFieldRefs();
        if (inputsFields.size() < 2) {
            throw new UDFArgumentException("RefSort takes at least 2 arguments");
        }
        inputInspectors = new ListObjectInspector[inputsFields.size()];
        for (int i = 0; i < inputsFields.size(); ++i) {
            inputInspectors[i] = (ListObjectInspector) inputsFields.get(i).getFieldObjectInspector();
        }
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        for (int i = 0; i < inputInspectors.length; i++) {
            fieldNames.add("array" + i);
            fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(inputInspectors[i].getListElementObjectInspector()));
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        List<?> refList = inputInspectors[0].getList(objects[0]);
        final ObjectInspector valInspector = inputInspectors[0].getListElementObjectInspector();
        final Integer[] sortIndex = IntStream.range(0, refList.size()).boxed().toArray(Integer[]::new);
        Arrays.sort(sortIndex, (o1, o2) -> ObjectInspectorUtils.compare(refList.get(o1), valInspector, refList.get(o2), valInspector));
        Object[] forwardObjects = new Object[objects.length];

        List<?> sortList;
        List<Object> retList;
        for (int i = 0; i < objects.length; i++) {
            sortList = inputInspectors[i].getList(objects[i]);
            retList = new ArrayList<>();
            for (int index: sortIndex) {
                retList.add(sortList.get(index));
            }
            forwardObjects[i] = retList;
        }
        forward(forwardObjects);
    }

    @Override
    public void close() throws HiveException {

    }
}
