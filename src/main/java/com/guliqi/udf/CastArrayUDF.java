package com.guliqi.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Cast an Array of objects to an Array of a different type
 * to avoid Hive UDF casting problems
 */
public class CastArrayUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(CastArrayUDF.class);
    private ListObjectInspector listInspector;
    private PrimitiveObjectInspector fromInspector;
    private PrimitiveObjectInspector toInspector;
    private String returnElemType;


    public List<Object> evaluate(List<Object> uninspArray) {
        List<Object> newList = new ArrayList<Object>();
        for (Object uninsp : uninspArray) {
            LOG.info("Uninspected = " + uninsp);
            Object stdObject = ObjectInspectorUtils.copyToStandardJavaObject(uninsp, fromInspector);
            Object castedObject = coerceObject(stdObject);
            newList.add(castedObject);
        }
        return newList;
    }

    private Object coerceObject(Object stdObj) {
        LOG.info("Casting " + stdObj + " from " + fromInspector.getPrimitiveCategory() + " to " + toInspector.getPrimitiveCategory() + " of type " + toInspector.getTypeName());
        if (stdObj == null) {
            return null;
        }
        switch (fromInspector.getPrimitiveCategory()) {
            case STRING:
                String fromStr = (String) stdObj;
                switch (toInspector.getPrimitiveCategory()) {
                    case STRING:
                        return fromStr;
                    case BOOLEAN:
                        if (fromStr.equals("true")) {
                            return Boolean.TRUE;
                        } else {
                            return Boolean.FALSE;
                        }
                    case BYTE:
                        /// XXX TODO
                    case SHORT:
                        return Short.parseShort(fromStr);
                    case INT:
                        return Integer.parseInt(fromStr);
                    case LONG:
                        return Long.parseLong(fromStr);
                    case FLOAT:
                        return Float.parseFloat(fromStr);
                    case DOUBLE:
                        return Double.parseDouble(fromStr);
                    case TIMESTAMP:
                        //// XXX TODO
                    case VOID:
                        return null;

                }
                return null;
            case SHORT:
            case INT:
            case FLOAT:
            case LONG:
            case DOUBLE:
                Number fromNum = (Number) stdObj;
                switch (toInspector.getPrimitiveCategory()) {
                    case SHORT:
                        return fromNum.shortValue();
                    case INT:
                        return fromNum.intValue();
                    case LONG:
                        return fromNum.longValue();
                    case FLOAT:
                        return fromNum.floatValue();
                    case DOUBLE:
                        return fromNum.doubleValue();
                    case STRING:
                        return fromNum.toString();
                    case TIMESTAMP:
                        //// XXX TODO
                    case BOOLEAN:
                        return fromNum.intValue() > 1? Boolean.TRUE: Boolean.FALSE;
                    case VOID:
                        return null;
                }
                return null;
        }
        return null;
    }

    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        List argList = listInspector.getList(arg0[0].get());
        if (argList != null)
            return evaluate(argList);
        else
            return null;
    }

    @Override
    public String getDisplayString(String[] arg0) {
        StringBuilder sb = new StringBuilder("cast_array(");
        sb.append(arg0[0]);
        if (arg0.length > 1) {
            sb.append(" , ");
            sb.append(arg0[1]);
        }
        return sb.toString();
    }

    private static PrimitiveObjectInspector GetObjectInspectorForTypeName(String typeString) {
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeString);
        LOG.info("Type for " + typeString + " is " + typeInfo);

        return (PrimitiveObjectInspector) TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if (arg0[0].getCategory() != Category.LIST) {
            throw new UDFArgumentException("cast_array() takes a list, and an optional type to cast to.");
        }
        this.listInspector = (ListObjectInspector) arg0[0];
        if (listInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE) {
            throw new UDFArgumentException("cast_array() only handles arrays of primitives.");
        }
        this.fromInspector = (PrimitiveObjectInspector) listInspector.getListElementObjectInspector();

        LOG.info(" Cast Array input type is " + listInspector + " element = " + listInspector.getListElementObjectInspector());
        if (arg0.length > 1) {
            if (!(arg0[1] instanceof ConstantObjectInspector)
                    || !(arg0[1] instanceof StringObjectInspector)) {
                throw new UDFArgumentException("cast_array() takes a list, and an optional type to cast to.");
            }
            ConstantObjectInspector constInsp = (ConstantObjectInspector) arg0[1];
            this.returnElemType = constInsp.getWritableConstantValue().toString();
            this.toInspector = GetObjectInspectorForTypeName(returnElemType);
            ObjectInspector returnType = ObjectInspectorFactory.getStandardListObjectInspector(toInspector);
            return returnType;
        }

        /// Otherwise, assume we're casting to strings ...
        this.returnElemType = "string";
        this.toInspector = GetObjectInspectorForTypeName(returnElemType);
        ObjectInspector returnType = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return returnType;
    }

//    public static void main(String[] args) {
//        try {
//            CastArrayUDF cau = new CastArrayUDF();
//            final String type = "boolean";
//            ObjectInspector stringOI = PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, new Text(type));
//            ObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
//            ListObjectInspector resultInspector = (ListObjectInspector) cau.initialize(new ObjectInspector[]{listOI, stringOI});
//            List<String> list = Arrays.asList("22", "99");
//            Object result = cau.evaluate(new DeferredObject[]{new DeferredJavaObject(list), new DeferredJavaObject(type)});
//            System.out.println(resultInspector.getList(result));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}