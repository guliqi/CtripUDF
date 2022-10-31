package com.guliqi.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.List;

public class SequenceBaggage extends UDF {
    public List<Double> evaluate(String flightNos, List<Double> baggageWeights) {
        String[] flightNoArray = flightNos.split(";|\\|\\|");
        if (flightNoArray.length != baggageWeights.size()) {
            return baggageWeights;
        }
        List<Double> result = new ArrayList<>();
        for (int i = 0; i < flightNoArray.length; i++) {
            for (int j = 0; j < flightNoArray[i].split("\\|").length; j++) {
                result.add(baggageWeights.get(i));
            }
        }
        return result;
    }
}
