package com.demo.springboot.mybatis.model;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class test {


    public static void main(String[] args){

        List<Person>  list = new ArrayList<>();

        list.add(new Person("a",20,1500,"java"));
        list.add(new Person("b",21,1500,"java"));
        list.add(new Person("c",22,1500,"java"));
        list.add(new Person("d",23,1000,"php"));
        list.add(new Person("e",24,1000,"php"));

        int jiaxin = 100;

        Consumer<Person> jiagongzi = person -> person.setSalary(person.getSalary() + jiaxin);

        list.forEach(jiagongzi);
        list.forEach(person -> System.out.println(person.toString()));


    }
}
