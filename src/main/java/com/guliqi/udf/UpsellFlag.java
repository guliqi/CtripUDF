package com.guliqi.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Arrays;
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
            return 2; // 全部段都是升级运价
        }
        if (partiallyTrue) {
            return 1;  // 部分段是升级运价
        }
        return 0;  // 全部段都不是升级运价
    }

    public static void main(String[] args) {
        UpsellFlag uf = new UpsellFlag();
        System.out.println(uf.evaluate(Arrays.asList(20.), Arrays.asList(0.), Arrays.asList(24.)));
    }
}