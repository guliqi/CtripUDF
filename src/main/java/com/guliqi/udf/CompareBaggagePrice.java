package com.guliqi.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.*;
import java.util.stream.DoubleStream;

public class CompareBaggagePrice extends GenericUDTF {

    private transient StringObjectInspector flightNosInspector;
    private transient ListObjectInspector freeBaggageWeightInspector;
    private transient DoubleObjectInspector farePriceInspector;
    private transient ListObjectInspector allSearchWeight2SalePriceInspector;
    private transient ListObjectInspector allSearchWeight2CostPriceInspector;
    private transient MapObjectInspector allFareWeight2FlightNosInspector;
    private transient MapObjectInspector allFareWeight2FarePriceInspector;

    private static int bisectLeft(double[] array, double key) {
        int low = 0;
        int high = array.length;

        while (low < high) {
            int mid = (low + high) / 2;
            if (array[mid] >= key){
                high = mid;
            }
            else{
                low = mid + 1;
            }
        }
        return low;
    }

    private Map<Integer, Integer> makeComparison(Map<Integer, Double> paidWeightPrice,
                                                Map<Integer, Double> fareWeightPrice,
                                                int[] fareBaggageWeight,
                                                double[] fareBaggagePrice) {
        double[] fareBaggageWeight_ = new double[fareBaggageWeight.length];
        for (int i = 0; i < fareBaggageWeight.length; ++i) {
            fareBaggageWeight_[i] = fareBaggageWeight[i];
        }
        Map<Integer, Integer> resultMap = new HashMap<>();
        for (Map.Entry<Integer, Double> entry: paidWeightPrice.entrySet()) {
            int paidWeight = entry.getKey();
            double paidPrice = entry.getValue();
            if (fareWeightPrice.containsKey(paidWeight)) {
                resultMap.put(paidWeight, Double.compare(fareWeightPrice.get(paidWeight), paidPrice));
            } else {
                int index = bisectLeft(fareBaggageWeight_, paidWeight);
                if (index > 0 && paidPrice <= fareBaggagePrice[index - 1]) {
                    resultMap.put(paidWeight, 1);
                } else if (index < fareBaggagePrice.length && paidPrice >= fareBaggagePrice[index]) {
                    resultMap.put(paidWeight, -1);
                } else {
                    resultMap.put(paidWeight, 0);
                }
            }
        }
        return resultMap;
    }

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<? extends StructField> inputsFields = argOIs.getAllStructFieldRefs();
        flightNosInspector = (StringObjectInspector) inputsFields.get(0).getFieldObjectInspector();
        freeBaggageWeightInspector = (ListObjectInspector) inputsFields.get(1).getFieldObjectInspector();
        farePriceInspector = (DoubleObjectInspector) inputsFields.get(2).getFieldObjectInspector();
        allSearchWeight2SalePriceInspector = (ListObjectInspector) inputsFields.get(3).getFieldObjectInspector();
        allSearchWeight2CostPriceInspector = (ListObjectInspector) inputsFields.get(4).getFieldObjectInspector();
        allFareWeight2FlightNosInspector = (MapObjectInspector) inputsFields.get(5).getFieldObjectInspector();
        allFareWeight2FarePriceInspector = (MapObjectInspector) inputsFields.get(6).getFieldObjectInspector();

        List<String> fieldNames = Arrays.asList("salePriceCompare", "costPriceCompare");
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
                ObjectInspectorFactory.getStandardMapObjectInspector(
                        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                        PrimitiveObjectInspectorFactory.javaIntObjectInspector)));
        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
                ObjectInspectorFactory.getStandardMapObjectInspector(
                        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                        PrimitiveObjectInspectorFactory.javaIntObjectInspector)));
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        String flightNos = flightNosInspector.getPrimitiveJavaObject(objects[0]);
        @SuppressWarnings("unchecked") List<Integer> freeBaggageWeight = (List<Integer>) freeBaggageWeightInspector.getList(objects[1]);
        Double farePrice = farePriceInspector.get(objects[2]);
        @SuppressWarnings("unchecked") List<Map<Integer, Double>> allSearchWeight2SalePrice = (List<Map<Integer, Double>>) allSearchWeight2SalePriceInspector.getList(objects[3]);
        @SuppressWarnings("unchecked") List<Map<Integer, Double>> allSearchWeight2CostPrice = (List<Map<Integer, Double>>) allSearchWeight2CostPriceInspector.getList(objects[4]);
        @SuppressWarnings("unchecked") Map<String, String> allFareWeight2FlightNos = (Map<String, String>) allFareWeight2FlightNosInspector.getMap(objects[5]);
        @SuppressWarnings("unchecked") Map<String, Double> allFareWeight2FarePrice = (Map<String, Double>) allFareWeight2FarePriceInspector.getMap(objects[6]);

        // 线性插值估算任意重量的行李额成本价作为运价划分比例
        Interpolator[] interpolators = new Interpolator[allSearchWeight2CostPrice.size()];
        for (int i = 0; i < allSearchWeight2CostPrice.size(); ++i) {
            Map<Integer, Double> weight2price = allSearchWeight2CostPrice.get(i);
            List<Integer> weightList = new ArrayList<Integer>(){{add(0);}};
            weightList.addAll(weight2price.keySet());
            double[] weightArray = weightList.stream().mapToDouble(Integer::doubleValue).toArray();
            Arrays.sort(weightArray);
            double[] priceArray = new double[weightArray.length];
            priceArray[0] = 0.;
            for (int j = 1; j < weightArray.length; ++j) {
                priceArray[j] = weight2price.get((int) weightArray[j]);
            }
            interpolators[i] = new Interpolator(weightArray, priceArray);
        }
        // 对于多段行程，划分运价，计算运价自带行李额的价值
        flightNos = flightNos.replace("||", ";");
        List<Map<Integer, Double>> fareBaggageValues = new ArrayList<>();
        for (int i = 0; i < freeBaggageWeight.size(); ++i) {
            fareBaggageValues.add(new HashMap<>());
        }
        for (Map.Entry<String, Double> weight2price: allFareWeight2FarePrice.entrySet()) {
            if (!allFareWeight2FlightNos.get(weight2price.getKey()).replace("||", ";").equals(flightNos)) {
                continue;
            }
            double[] segmentWeights = Arrays.stream(weight2price.getKey().split(",")).mapToDouble(Double::parseDouble).toArray();
            double[] segmentSplitRatio = new double[segmentWeights.length];
            for (int i = 0; i < segmentWeights.length; ++i) {
                segmentSplitRatio[i] = interpolators[i].interpolate(segmentWeights[i]);
            }
            double costPriceSum = DoubleStream.of(segmentSplitRatio).sum();
            for (int i = 0; i < segmentSplitRatio.length; ++i) {
                segmentSplitRatio[i] /= costPriceSum;
                int weightDiff = (int) segmentWeights[i] - freeBaggageWeight.get(i);
                if (weightDiff > 0) {
                    double priceDiff = Math.round(Double.max(0, (weight2price.getValue() - farePrice) * segmentSplitRatio[i]));
                    if (fareBaggageValues.get(i).containsKey(weightDiff)) {
                        fareBaggageValues.get(i).replace(weightDiff, Double.min(priceDiff, fareBaggageValues.get(i).get(weightDiff)));
                    } else {
                        fareBaggageValues.get(i).put(weightDiff, priceDiff);
                    }
                }
            }
        }
        // 计算付费行李额优劣势
        List<Map<Integer, Integer>> salePriceCompareResult = new ArrayList<>();
        List<Map<Integer, Integer>> costPriceCompareResult = new ArrayList<>();
        for (int i = 0; i < fareBaggageValues.size(); ++i) {
            Map<Integer, Double> segmentSalePrice = allSearchWeight2SalePrice.get(i);
            Map<Integer, Double> segmentCostPrice = allSearchWeight2CostPrice.get(i);
            Map<Integer, Double> segmentFareBaggageValue = fareBaggageValues.get(i);

            int[] segmentFareBaggageWeight = segmentFareBaggageValue.keySet().stream().mapToInt(Integer::intValue).toArray();
            Arrays.sort(segmentFareBaggageWeight);
            double[] segmentFareBaggagePrice = new double[segmentFareBaggageWeight.length];
            for (int j = 0; j < segmentFareBaggagePrice.length; ++j) {
                segmentFareBaggagePrice[j] = segmentFareBaggageValue.get(segmentFareBaggageWeight[j]);
            }
            salePriceCompareResult.add(makeComparison(segmentSalePrice, segmentFareBaggageValue, segmentFareBaggageWeight, segmentFareBaggagePrice));
            costPriceCompareResult.add(makeComparison(segmentCostPrice, segmentFareBaggageValue, segmentFareBaggageWeight, segmentFareBaggagePrice));
        }
        Object[] forwardObjects = new Object[2];
        forwardObjects[0] = salePriceCompareResult;
        forwardObjects[1] = costPriceCompareResult;
        forward(forwardObjects);
    }

    @Override
    public void close() throws HiveException {

    }

//    public static void main(String[] args) {
//        CompareBaggagePrice example = new CompareBaggagePrice();
//        List<String> fieldNames = Arrays.asList("salePriceCompare", "costPriceCompare");
//        List<ObjectInspector> fieldOIs = new ArrayList<>();
//        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
//                ObjectInspectorFactory.getStandardMapObjectInspector(
//                        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
//                        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector)));
//        fieldOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(
//                ObjectInspectorFactory.getStandardMapObjectInspector(
//                        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
//                        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector)));
//        StructObjectInspector inspector = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
//        System.out.println(inspector.getAllStructFieldRefs());
//    }
}

class Interpolator {
    private final double[] xList;
    private final double[] yList;
    private final double[] slopes;

    Interpolator(double[] xList, double[] yList) {
        this.xList = xList;
        this.yList = yList;
        slopes = new double[xList.length];
        int i;
        for (i = 0; i < xList.length - 1; ++i) {
            slopes[i] = (yList[i + 1] - yList[i]) / (xList[i + 1] - xList[i]);
        }
        slopes[i] = slopes[i - 1];
    }

    static int bisectRight(double[] array, double key) {
        int low = 0;
        int high = array.length;

        while (low < high) {
            int mid = (low + high) / 2;
            if (array[mid] > key){
                high = mid;
            }
            else{
                low = mid + 1;
            }
        }
        return low;
    }

    double interpolate(double x) {
        if (x < 0) {
            return 0;
        }
        int i = bisectRight(this.xList, x) - 1;
        return yList[i] + slopes[i] * (x - xList[i]);
    }
}