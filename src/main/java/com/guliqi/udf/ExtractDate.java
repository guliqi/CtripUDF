package com.guliqi.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class ExtractDate extends UDF {
    public String evaluate(String takeofftimes) {
        String[] times = takeofftimes.split(",");
        for (int i = 0; i < times.length; i++) {
            times[i] = times[i].substring(0, 10);
        }
        return String.join(",", times);
    }
}
