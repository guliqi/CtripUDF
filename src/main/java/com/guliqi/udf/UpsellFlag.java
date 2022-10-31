package com.guliqi.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.List;

public class UpsellFlag extends UDF {
    public Integer evaluate(List<Double> freeBaggageWeight, List<Double> XBaggageWeight, List<Double> upsellBaggageWeight) {
        if ((freeBaggageWeight.size() != XBaggageWeight.size()) || (XBaggageWeight.size() != upsellBaggageWeight.size())) {
            return null;
        }
        if (freeBaggageWeight.equals(upsellBaggageWeight)) {
            return -1;
        }
        boolean isUpsell;
        boolean allTrue = true;
        boolean partiallyTrue = false;
        for (int i = 0; i < freeBaggageWeight.size(); ++i) {
            double XWeight = XBaggageWeight.get(i);
            if (XWeight == 0) {
                XWeight = 0.1;
            }
            isUpsell = freeBaggageWeight.get(i) + XWeight <= upsellBaggageWeight.get(i);
            allTrue &= isUpsell;
            partiallyTrue |= isUpsell;
        }
        if (allTrue) {
            return 2;
        }
        if (partiallyTrue) {
            return 1;
        }
        return 0;
    }
}