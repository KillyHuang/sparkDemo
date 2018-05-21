package com.spark.demo.jdbc;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD;
import scala.Function0;

import java.sql.*;

public class JDBCRDD_create implements Function0<Connection>{

    @Override
    public Connection apply() {
        return null;
    }

    @Override
    public boolean apply$mcZ$sp() {
        return false;
    }

    @Override
    public byte apply$mcB$sp() {
        return 0;
    }

    @Override
    public char apply$mcC$sp() {
        return 0;
    }

    @Override
    public double apply$mcD$sp() {
        return 0;
    }

    @Override
    public float apply$mcF$sp() {
        return 0;
    }

    @Override
    public int apply$mcI$sp() {
        return 0;
    }

    @Override
    public long apply$mcJ$sp() {
        return 0;
    }

    @Override
    public short apply$mcS$sp() {
        return 0;
    }

    @Override
    public void apply$mcV$sp() {

    }

    private Connection connection;
    private JDBCRDD_create(){}

    public Connection createConnection(){
        try {
            Class.forName("").newInstance();
            connection = DriverManager.getConnection("jdbc:mysql://localhost/test?characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull");
        }catch (Exception e){
            e.printStackTrace();
        }
        return connection;
    }

    public void getResultSet(){

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.getResultSet();
            resultSet.getString(1);
        }catch (SQLException e){

        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("");
        SparkContext context = new SparkContext(conf);
        //JDBCRDD jdbcrdd = new JDBCRDD(context);

    }



}
