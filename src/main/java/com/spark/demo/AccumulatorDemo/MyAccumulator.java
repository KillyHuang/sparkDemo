package com.spark.demo.AccumulatorDemo;

import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.AccumulableInfo;
import org.apache.spark.util.AccumulatorMetadata;
import org.apache.spark.util.AccumulatorV2;
import scala.Option;

/**
 * 实现一个累加器，对输入的日志信息进行规则累加计算
 * spark中累加器的执行流程：
 *
 * 首先有几个task，spark engine就调用copy方法拷贝几个累加器（不注册的）
 * 然后在各个task中进行累加（注意在此过程中，被最初注册的累加器的值是不变的）
 * 执行最后将调用merge方法和各个task的结果累计器进行合并（此时被注册的累加器是初始值）
 */
public class MyAccumulator extends AccumulatorV2<String,String>{

    public MyAccumulator() {
        super();
    }

    @Override
    public AccumulatorMetadata metadata() {
        return super.metadata();
    }

    @Override
    public void metadata_$eq(AccumulatorMetadata x$1) {
        super.metadata_$eq(x$1);
    }

    @Override
    public void register(SparkContext sc, Option<String> name, boolean countFailedValues) {
        super.register(sc, name, countFailedValues);
    }

    @Override
    public Option<String> register$default$2() {
        return super.register$default$2();
    }

    @Override
    public boolean register$default$3() {
        return super.register$default$3();
    }

    @Override
    public AccumulableInfo toInfo(Option<Object> update, Option<Object> value) {
        return super.toInfo(update, value);
    }

    @Override
    public AccumulatorV2<String, String> copyAndReset() {
        return super.copyAndReset();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public boolean isZero() {
        return false;
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        return null;
    }

    @Override
    public void reset() {
    }

    @Override
    public void add(String s) {
    }

    @Override
    public void merge(AccumulatorV2<String, String> accumulatorV2) {

    }

    @Override
    public String value() {
        return null;
    }
}
