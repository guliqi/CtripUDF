package com.guliqi.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class SupInfCheck extends UDF {
    public Integer evaluate(double price, List<Double> freeBaggageWeight, List<Double> paidBaggageWeight,
                            List<Double> paidBaggagePrice, Map<String, Double> aggWeight2Price) {
        // price = 200, freeBaggageWeight = [10, 10], paidBaggageWeight = [5, 0], paidBaggagePrice = [80, 0], aggWeight2Price = {"0,0": 150, "10,10": 200, "15,10": 260}
        boolean zeroWeight = true;
        for (double weight: paidBaggageWeight) {
            if (weight > 0) {
                zeroWeight = false;
                break;
            }
        }
        if (zeroWeight) {
            return null;  // 没有覆盖
        }

        double[] sumWeight = new double[freeBaggageWeight.size()];
        for (int i = 0; i < freeBaggageWeight.size(); ++i) {
            sumWeight[i] = freeBaggageWeight.get(i) + paidBaggageWeight.get(i);
        }
        double sumPrice = price;
        for (double paidPrice: paidBaggagePrice) {
            sumPrice += paidPrice;
        }

        for (Map.Entry<String, Double> entry: aggWeight2Price.entrySet()) {
            double[] aggWeight = Stream.of(entry.getKey().split(",")).mapToDouble(Double::parseDouble).toArray();
            double aggPrice = entry.getValue();
            if (Arrays.equals(sumWeight, aggWeight)) {
                return Double.compare(aggPrice, sumPrice);
            }
            boolean allLess = true;  // 所有段比agg都轻
            boolean allGreater = true;  // 所有段比agg都重
            for (int i = 0; i < aggWeight.length; ++i) {
                if (sumWeight[i] < aggWeight[i]) {
                    allGreater = false;
                }
                else if (sumWeight[i] > aggWeight[i]) {
                    allLess = false;
                }
            }
            if (allGreater && sumPrice <= aggPrice) {
                return 1;
            }
            if (allLess && sumPrice >= aggPrice) {
                return -1;
            }
        }
        return 0;
    }
}