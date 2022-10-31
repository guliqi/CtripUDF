package com.guliqi.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.lang.reflect.Method;
import java.util.*;

public class VectorCompare extends UDF {
    private static final Map<String, String> methods = new HashMap<>();
    static {
        methods.put(">", "greater");
        methods.put("<", "less");
        methods.put("=", "equal");
        methods.put(">=", "greaterOrEqual");
        methods.put("<=", "lessOrEqual");
    }
    private static final Set<String> modes = new HashSet<>(Arrays.asList("all", "any"));
    private static boolean greater(double a, double b) {
        return a > b;
    }
    private static boolean less(double a, double b) {
        return a < b;
    }
    private static boolean equal(double a, double b) {
        return a == b;
    }
    private static boolean greaterOrEqual(double a, double b) {
        return a >= b;
    }
    private static boolean lessOrEqual(double a, double b) {
        return a <= b;
    }
    public Boolean evaluate(List<Double> vector1, List<Double> vector2, String symbol, String mode) {
        if (!methods.containsKey(symbol) || !modes.contains(mode) || vector1.size() != vector2.size()){
            return null;
        }
        try {
            boolean andResult = true;
            boolean orResult = false;
            Method method = VectorCompare.class.getDeclaredMethod(methods.get(symbol), double.class, double.class);
            for (int i = 0; i < vector1.size(); ++i) {
                boolean res = (boolean) method.invoke(this, vector1.get(i), vector2.get(i));
                if (mode.equals("any") && res) {
                    return true;
                }
                if (mode.equals("all") && !res) {
                    return false;
                }
                andResult &= res;
                orResult |= res;
            }
            return mode.equals("any") ? orResult : andResult;
        } catch (Exception e) {
            return null;
        }
    }
}
