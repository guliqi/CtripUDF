package com.guliqi.udf;

/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Add two vectors together
 */
@Description(
        name = "vector_add",
        value = " Add two vectors together"
)
public class VectorAddUDF extends GenericUDF {
    private static final Logger LOG = Logger.getLogger(VectorAddUDF.class);
    private ListObjectInspector list1Inspector;
    private ListObjectInspector list2Inspector;
    private MapObjectInspector map1Inspector;
    private MapObjectInspector map2Inspector;
    private PrimitiveObjectInspector key1Inspector;
    private PrimitiveObjectInspector key2Inspector;
    private PrimitiveObjectInspector value1Inspector;
    private PrimitiveObjectInspector value2Inspector;

    private StandardListObjectInspector retListInspector;
    private StandardMapObjectInspector retMapInspector;


    public Object evaluateList(Object list1Obj, Object list2Obj) {
        int len1 = list1Inspector.getListLength(list1Obj);
        int len2 = list2Inspector.getListLength(list2Obj);
        if (len1 != len2) {
            LOG.warn("vector lengths do not match " + list1Obj + " :: " + list2Obj);
            return null;
        }
        Object retList = retListInspector.create(len1); // ??????bug
        for (int i = 0; i < len1; ++i) {
            Object list1Val = this.list1Inspector.getListElement(list1Obj, i);
            double list1Dbl = NumericUtil.getNumericValue(value1Inspector, list1Val);
            Object list2Val = this.list2Inspector.getListElement(list2Obj, i);
            double list2Dbl = NumericUtil.getNumericValue(value2Inspector, list2Val);

            double newVal = list1Dbl + list2Dbl;
            retListInspector.set(retList, i, NumericUtil.castToPrimitiveNumeric(newVal,
                    ((PrimitiveObjectInspector) retListInspector.getListElementObjectInspector()).getPrimitiveCategory()));
        }
        return retList;
    }

    public Object evaluateMap(Object uninspMapObj1, Object uninspMapObj2) {
        /// A little tricky, because keys won't match if the ObjectInspectors aren't the
        /// same .. If the ObjectInspectors are the same class, assume they can be compared
        Object retMap = retMapInspector.create();
        Map map1 = map1Inspector.getMap(uninspMapObj1);
        Map map2 = map2Inspector.getMap(uninspMapObj2);
        Map stdKeyMap = new HashMap();
        for (Object mapKey2 : map2.keySet()) {
            Object stdKey2 = ObjectInspectorUtils.copyToStandardJavaObject(mapKey2,
                    map2Inspector.getMapKeyObjectInspector());
            stdKeyMap.put(stdKey2, mapKey2);
        }

        for (Object mapKey1 : map1.keySet()) {
            Object mapVal1Obj = map1.get(mapKey1);
            double mapVal1Dbl = NumericUtil.getNumericValue(value1Inspector, mapVal1Obj);

            Object stdKey1 = ObjectInspectorUtils.copyToStandardJavaObject(mapKey1,
                    map1Inspector.getMapKeyObjectInspector());

            Object lookupKey = stdKeyMap.get(stdKey1);

            if (lookupKey != null) {
                Object mapVal2Obj = map2.get(lookupKey);
                double mapVal2Dbl = NumericUtil.getNumericValue(value2Inspector, mapVal2Obj);
                double newVal = mapVal1Dbl + mapVal2Dbl;
                stdKeyMap.remove(stdKey1);

                Object stdVal = NumericUtil.castToPrimitiveNumeric(newVal,
                        ((PrimitiveObjectInspector) retMapInspector.getMapValueObjectInspector()).getPrimitiveCategory());
                retMapInspector.put(retMap, stdKey1, stdVal);
            } else {
                /// Add the dimension, even if it wasn't in the second vector
                Object stdVal = NumericUtil.castToPrimitiveNumeric(mapVal1Dbl,
                        ((PrimitiveObjectInspector) retMapInspector.getMapValueObjectInspector()).getPrimitiveCategory());
                retMapInspector.put(retMap, stdKey1, stdVal);
            }

        }
        /// Add all the values which were in map2, but not in map1
        for (Object leftOverKey : stdKeyMap.keySet()) {
            Object leftOverVal = map2.get(stdKeyMap.get(leftOverKey));
            double leftOverDbl = NumericUtil.getNumericValue(value2Inspector, leftOverVal);
            Object stdVal = NumericUtil.castToPrimitiveNumeric(leftOverDbl,
                    ((PrimitiveObjectInspector) retMapInspector.getMapValueObjectInspector()).getPrimitiveCategory());
            retMapInspector.put(retMap, leftOverKey, stdVal);
        }
        return retMap;
    }


    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        if (list1Inspector != null) {
            return evaluateList(arg0[0].get(), arg0[1].get());
        } else {
            return evaluateMap(arg0[0].get(), arg0[1].get());
        }
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "vector_cross_product";
    }


    private void usage(String message) throws UDFArgumentException {
        LOG.error("vector_cross_product: Multiply a vector times another vector : " + message);
        throw new UDFArgumentException("vector_scalar_mult: Multiply a vector times another vector : " + message);
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if (arg0.length != 2)
            usage("Must have two arguments.");

        if (arg0[0].getCategory() == Category.MAP) {
            if (arg0[1].getCategory() != Category.MAP)
                usage("Vectors need to be both maps");
            this.map1Inspector = (MapObjectInspector) arg0[0];
            this.map2Inspector = (MapObjectInspector) arg0[1];

            if (map1Inspector.getMapKeyObjectInspector().getCategory() != Category.PRIMITIVE)
                usage("First Vector map key must be a primitive");
            this.key1Inspector = (PrimitiveObjectInspector) map1Inspector.getMapKeyObjectInspector();

            if (map2Inspector.getMapKeyObjectInspector().getCategory() != Category.PRIMITIVE)
                usage("Second Vector map key must be a primitive");
            this.key2Inspector = (PrimitiveObjectInspector) map2Inspector.getMapKeyObjectInspector();

            if (key2Inspector.getPrimitiveCategory() != key1Inspector.getPrimitiveCategory())
                usage(" Map key types must match");

            if (map1Inspector.getMapValueObjectInspector().getCategory() != Category.PRIMITIVE)
                usage("First Vector map value must be a primitive");
            this.value1Inspector = (PrimitiveObjectInspector) map1Inspector.getMapValueObjectInspector();

            if (map2Inspector.getMapValueObjectInspector().getCategory() != Category.PRIMITIVE)
                usage("Second Vector map value must be a primitive");
            this.value2Inspector = (PrimitiveObjectInspector) map2Inspector.getMapValueObjectInspector();


        } else if (arg0[0].getCategory() == Category.LIST) {
            if (arg0[1].getCategory() != Category.LIST)
                usage("Vectors need to be both arrays");
            this.list1Inspector = (ListObjectInspector) arg0[0];
            this.list2Inspector = (ListObjectInspector) arg0[1];

            if (list1Inspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE)
                usage("First Vector array value must be a primitive");
            this.value1Inspector = (PrimitiveObjectInspector) list1Inspector.getListElementObjectInspector();

            if (list2Inspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE)
                usage("Second Vector array value must be a primitive");
            this.value2Inspector = (PrimitiveObjectInspector) list2Inspector.getListElementObjectInspector();

        } else {
            usage("Arguments must be arrays or maps");
        }


        if (list1Inspector != null) {
            retListInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                    ObjectInspectorUtils.getStandardObjectInspector(value1Inspector,
                            ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA));
            return retListInspector;
        } else {
            retMapInspector = ObjectInspectorFactory.getStandardMapObjectInspector(
                    ObjectInspectorUtils.getStandardObjectInspector(map1Inspector.getMapKeyObjectInspector(),
                            ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA),
                    ObjectInspectorUtils.getStandardObjectInspector(value1Inspector,
                            ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA));
            return retMapInspector;
        }
    }
}
