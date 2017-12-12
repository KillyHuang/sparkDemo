package com.spark.demo.filehandle;

import com.alibaba.fastjson.JSONObject;
import com.spark.demo.bean.Person;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WriteJson implements FlatMapFunction<Iterator<Person>,String> {
    @Override
    public Iterator<String> call(Iterator<Person> people) throws Exception {
        List<String> lines = new ArrayList<>();
        while(people.hasNext()){
            Person person = people.next();
            try {
                String line = JSONObject.toJSONString(person);
                lines.add(line);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return lines.iterator();
    }
}
