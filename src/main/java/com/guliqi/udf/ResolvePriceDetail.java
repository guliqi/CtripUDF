package com.guliqi.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import static java.lang.Math.round;

public class ResolvePriceDetail extends UDF {
    public List<Long> evaluate(String originalPriceDetail, Map<String, Double> exchangeRateMap) {
        String[] detailArray = originalPriceDetail.split(",");
        List<Long> result = new ArrayList<>();
        for (String s : detailArray) {
            String[] detail = s.split("\\|");
            double totalPrice = Double.parseDouble(detail[1]) + Double.parseDouble(detail[2]);
            if (detail[0].equals("CNY")) {
                result.add(round(totalPrice));
            } else {
                double exchangeRate = exchangeRateMap.get(detail[0]);
                result.add(round(exchangeRate * totalPrice));
            }
        }
        return result;
    }
}
