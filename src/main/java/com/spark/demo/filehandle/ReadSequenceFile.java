package com.spark.demo.filehandle;

import org.apache.hadoop.io.IntWritable;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import javax.xml.soap.Text;

public class ReadSequenceFile implements PairFunction<Tuple2<Text,IntWritable>,String,Integer> {
    @Override
    public Tuple2<String, Integer> call(Tuple2<Text, IntWritable> tuple) throws Exception {
        Tuple2<String ,Integer> result = new Tuple2(tuple._1(),tuple._2());
        return result;
    }


    /**
     * 保存SequentFile的时候，可以讲元素转化为键值对的数组形式进行封装，然后将其保存成:saveAsSequenceFile
     */
}
