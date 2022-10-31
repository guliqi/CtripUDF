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
        for (String baggageDetail: details) {
            Matcher matcher = pattern.matcher(baggageDetail.toUpperCase());
            if (matcher.find()) {
                int piece = Integer.parseInt(matcher.group(1));
                int weight = Integer.parseInt(matcher.group(2));
                result.add(Math.max(piece, 1) * Math.max(weight, 0));
            }
            else {
                result.add(0);
            }
        }
        return result;
    }
}
