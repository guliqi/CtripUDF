package com.guliqi.udf;
import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.List;

public class CheckMonotonic extends UDF {
    public Boolean evaluate(List<Double> array, int tone) {
        if (tone == 1) {
            for (int i = 1; i < array.size(); ++i) {
                if (array.get(i - 1) >= array.get(i)) {
                    return false;
                }
            }
            return true;
        }
        if (tone == -1) {
            for (int i = 1; i < array.size(); ++i) {
                if (array.get(i - 1) <= array.get(i)) {
                    return false;
                }
            }
            return true;
        }
        return null;
    }
}
