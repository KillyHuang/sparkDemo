package com.spark.demo.filehandle;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义的数据输入与输出类型
 */
public class MyWritableComparable implements WritableComparable<MyWritableComparable> {

    public static final int code = 13;

    private String name;
    private Long rule;

    public MyWritableComparable(){
        name = new String();
        rule = new Long(0);
    }

    public MyWritableComparable(String name, Long rule) {
        this.name = name;
        this.rule = rule;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getRule() {
        return rule;
    }

    public void setRule(Long rule) {
        this.rule = rule;
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = code * result + ((name == null)? 0 : name.hashCode());
        result = code * result + ((rule == 0)? 0 : rule.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        //类型相等
        if (obj == this)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        MyWritableComparable comparable = (MyWritableComparable)obj;
        //判断值相等
        if(this.name == null){
            if (comparable.getName() != null){
                return false;
            }
        }else if ( !name.equals(comparable.getName())){
            return  false;
        }
        if(this.rule == null){
            if(comparable.getRule() != null){
                return  false;
            }
        }else if( !this.rule.equals(comparable.getRule()) ){
            return false;
        }

        return true;
    }

    @Override
    public int compareTo(MyWritableComparable o) {
        int cmp = this.name.compareTo(o.getName());
        if(cmp != 0){
            return cmp;
        }
        return this.rule.compareTo(o.getRule());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeLong(rule);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        name = dataInput.readUTF();
        rule = dataInput.readLong();
    }
}
