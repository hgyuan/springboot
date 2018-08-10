package com.demo.springboot.mybatis.model;

public class Person {
    private String name;
    private int age;
    private int salary;
    private String job;

    public Person(String name,int age,int salary,String job){
        this.name = name;
        this.age = age;
        this.salary = salary;
        this.job = job;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String toString(){
        return this.getName() + " " +this.getAge() + " " + this.getSalary()+ " " + this.getJob();
    }
}
