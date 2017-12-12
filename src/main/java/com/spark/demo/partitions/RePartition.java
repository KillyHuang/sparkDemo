package com.spark.demo.partitions;

import org.apache.spark.Partitioner;

import java.net.URL;

/**
 * 数据格式：{pageID,pageURL}
 * 假如我们想把来自同一个域名的URL放到一台节点上，比如:http://www.iteblog.com和http://www.iteblog.com/archives/1368
 * 如果你使用HashPartitioner，这两个URL的Hash值可能不一样，这就使得这两个URL被放到不同的节点上
 * 所以这种情况下我们就需要自定义我们的分区策略
 */
public class RePartition  extends Partitioner{
    private int number;

    public RePartition() {
    }

    public RePartition(int number) {
        this.number = number;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    @Override
    public int numPartitions() {
        return number;
    }

    @Override
    public int getPartition(Object o) {
        try {
            String host = new URL(o.toString()).getHost();
            int code = host.hashCode() % number;
            if(code < 0){
                return code + number;
            }else{
                return code;
            }
        }catch (Exception e){
            e.printStackTrace();
            return 0;
        }
    }

     @Override
     public boolean equals(Object obj) {
         RePartition rePartition = (RePartition)obj;
         if(rePartition.getNumber() == this.number){
             return true;
         }else{
             return false;
         }
     }
     /**
      * numPartitions：这个方法需要返回你想要创建分区的个数；
     * getPartition：这个函数需要对输入的key做计算，然后返回该key的分区ID，范围一定是0到numPartitions-1；
     * equals()：这个是Java标准的判断相等的函数，之所以要求用户实现这个函数是因为Spark内部会比较两个RDD的分区是否一样。
     */
}
