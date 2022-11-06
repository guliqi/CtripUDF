package com.guliqi.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.ArrayList;
import java.util.List;


public class VectorMaxUDAF extends UDAF {
    public static class MeansUDAFEvaluator implements UDAFEvaluator {
        private final List<Double> list;

        public MeansUDAFEvaluator() {
            this.list = new ArrayList<>();
            init();
        }

        // 1. 初始化
        @Override
        public void init() {

        }

        // 2. 负责接收并处理一条输入数据
        public boolean iterate(List<Double> nextList) {
            if (nextList == null) {
                return false;
            }
            for (int i = 0; i < nextList.size(); ++i) {
                if (i >= this.list.size()) {
                    this.list.add(nextList.get(i));
                } else {
                    this.list.set(i, Double.max(this.list.get(i), nextList.get(i)));
                }
            }
            return true;
        }

        // 3. 处理map的输出结果
        public List<Double> terminatePartial() {
            return this.list;
        }

        // 4. 负责融合处理中间结果
        public boolean merge(List<Double> otherList) {
            if (otherList == null) {
                return false;
            }
            for (int i = 0; i < otherList.size(); ++i) {
                if (i >= this.list.size()) {
                    this.list.add(otherList.get(i));
                } else {
                    this.list.set(i, Double.max(this.list.get(i), otherList.get(i)));
                }
            }
            return true;
        }

        // 5. 给出最后的结果
        public List<Double> terminate() {
            return this.list;
        }

    }
}
