package com.spark.demo.bean;

public class Person {
    private String name;
    private String address;
    private int phone;
    private int age;
    private String email;

    public Person(){}

    public Person(String name, String address, int phone, int age, String email) {
        this.name = name;
        this.address = address;
        this.phone = phone;
        this.age = age;
        this.email = email;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPhone() {
        return phone;
    }

    public void setPhone(int phone) {
        this.phone = phone;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
}
