package com.guliqi.udf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.hadoop.hive.ql.exec.UDF;

public class BaggageSeparatorReplace extends UDF{
    private static final Pattern SEPARATOR = Pattern.compile("\\|{2}|;");

    public String evaluate(String flightNos, String baggageDetail) {
        Matcher matcher = SEPARATOR.matcher(flightNos);
        List<String> separators = new ArrayList<>();
        while (matcher.find()) {
            separators.add(matcher.group());
        }
        String[] details = new String[separators.size() + 1];
        Arrays.fill(details, "0:0PC+0KG");
        if (baggageDetail.length() > 0) {
            String[] baggageDetailList = baggageDetail.split("\\|");
            System.arraycopy(baggageDetailList, 0, details, 0, Math.min(details.length, baggageDetailList.length));
        }

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < details.length; i++) {
            try {
                String baggageWeight = details[i].split(":", 2)[1].split("\\+", 2)[1];
                int weight = Integer.parseInt(baggageWeight.toUpperCase().replace("KG", ""));
                result.append(weight > 0 ? String.valueOf(weight) : "0");
            } catch (Exception e) {
                result.append("0");
            }
            if (i < separators.size()) {
                result.append(separators.get(i));
            }
        }
        return result.toString();
    }
}
