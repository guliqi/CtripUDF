package com.guliqi.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.List;

public class ResolveFreeWeight extends UDF {
    public List<Integer> evaluate(List<Integer> freeBaggagePiece, List<Integer> freeBaggageWeight) {
        int defaultWeight = 23;
        int maxWeight = 0;
        for (Integer weight: freeBaggageWeight) {
            if (weight > maxWeight) {
                maxWeight = weight;
                defaultWeight = maxWeight;
            }
        }
        List<Integer> freeBaggageTotalWeight = new ArrayList<>();
        for (int i = 0; i < freeBaggagePiece.size(); ++i) {
            if (freeBaggagePiece.get(i) <= 0 && freeBaggageWeight.get(i) <= 0) {
                freeBaggageTotalWeight.add(0);
            } else if (freeBaggagePiece.get(i) <= 0) {
                freeBaggageTotalWeight.add(freeBaggageWeight.get(i));
            } else if (freeBaggageWeight.get(i) <= 0) {
                freeBaggageTotalWeight.add(freeBaggagePiece.get(i) * defaultWeight);
            } else {
                freeBaggageTotalWeight.add(freeBaggageWeight.get(i) * freeBaggagePiece.get(i));
            }
        }
        return freeBaggageTotalWeight;
    }
}
