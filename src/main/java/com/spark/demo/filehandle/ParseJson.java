package com.spark.demo.filehandle;

import com.alibaba.fastjson.JSONObject;
import com.spark.demo.bean.Person;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;

public class ParseJson implements FlatMapFunction<Iterator<String>,Person> {
    @Override
    public Iterator<Person> call(Iterator<String> lines) throws Exception {
        ArrayList<Person> people = new ArrayList<>();
        while(lines.hasNext()){
            String line = lines.next();
            try {
                JSONObject json = JSONObject.parseObject(line);
                Person person = JSONObject.toJavaObject(json,Person.class);
                people.add(person);
            }catch (Exception e){
                System.out.println("JSON string 转化失败");
                e.printStackTrace();
            }
        }
        return people.iterator();
    }

}
