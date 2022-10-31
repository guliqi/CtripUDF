package com.guliqi.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.List;

public class UpgradeWeightFlag extends UDF {
    public Integer evaluate(List<Double> freeBaggageWeight, List<Double> XBaggageWeight, List<Double> upgradeBaggageWeight) {
        if ((freeBaggageWeight.size() != XBaggageWeight.size()) || (XBaggageWeight.size() != upgradeBaggageWeight.size())) {
            return null;
        }
        if (freeBaggageWeight.equals(upgradeBaggageWeight)) {
            return -1;
        }
        boolean isUpgradeWeight;
        boolean allTrue = true;
        boolean partiallyTrue = false;
        for (int i = 0; i < freeBaggageWeight.size(); ++i) {
            double XWeight = XBaggageWeight.get(i);
            if (XWeight == 0) {
                XWeight = 0.1;
            }
            isUpgradeWeight = freeBaggageWeight.get(i) + XWeight <= upgradeBaggageWeight.get(i);
            allTrue &= isUpgradeWeight;
            partiallyTrue |= isUpgradeWeight;
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
