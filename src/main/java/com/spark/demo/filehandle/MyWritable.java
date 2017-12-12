package com.spark.demo.filehandle;

import com.spark.demo.bean.Person;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyWritable implements Writable {
    /**
     * Hadoop序列化机制中还包含另外几个重要的接口：WritableComparable、RawComparator 和 WritableComparator
     * @param dataOutput
     * @throws IOException
     */
    private Text name;
    private Text rule;

    public MyWritable(){
        this.name = new Text();
        this.rule = new Text();
    }

    public MyWritable(Text name, Text rule) {
        this.name = name;
        this.rule = rule;
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public Text getRule() {
        return rule;
    }

    public void setRule(Text rule) {
        this.rule = rule;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        name.write(dataOutput);
        rule.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        name.readFields(dataInput);
        rule.readFields(dataInput);
    }
}
