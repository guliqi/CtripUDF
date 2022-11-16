package com.guliqi.udf;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ResolveBaggageDetail extends UDF {
    private final Pattern pattern = Pattern.compile("^[01]:(-?\\d+)PC\\+(-?\\d+)");

    public List<Integer> evaluate(String checkedBaggageDetailType, int Length) {
        List<Integer> result = new ArrayList<>();
        String[] details = new String[Length];
        Arrays.fill(details, "0:0PC+0KG");

        if (checkedBaggageDetailType.length() > 0) {
            String[] baggageDetailList = checkedBaggageDetailType.split("\\|");
            System.arraycopy(baggageDetailList, 0, details, 0, Math.min(details.length, baggageDetailList.length));
        }
        List<Integer> pieces = new ArrayList<>();
        List<Integer> weights = new ArrayList<>();
        int maxWeight = 0;
        int defaultWeight = 23;  // 修正 piece > 0 && weight = -1 的情况
        for (String baggageDetail: details) {
            Matcher matcher = pattern.matcher(baggageDetail.toUpperCase());
            int piece;
            int weight;
            if (matcher.find()) {
                piece = Integer.parseInt(matcher.group(1));
                weight = Integer.parseInt(matcher.group(2));
                if (piece > 5) {  // 异常数据
                    piece = 1;
                }
            }
            else {
                piece = 0;
                weight = 0;
            }
            pieces.add(piece);
            weights.add(weight);
            if (weight > maxWeight) {
                maxWeight = weight;
                defaultWeight = maxWeight;
            }
        }
        for (int i = 0; i < details.length; ++i) {
            if (pieces.get(i) <= 0 && weights.get(i) <= 0) {
                result.add(0);
            } else if (pieces.get(i) <= 0) {
                result.add(weights.get(i));
            } else if (weights.get(i) <= 0) {
                result.add(pieces.get(i) * defaultWeight);
            } else {
                result.add(pieces.get(i) * weights.get(i));
            }
        }
        return result;
    }
}
