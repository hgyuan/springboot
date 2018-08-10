package com.demo.springboot.mybatis.dao;

import com.demo.springboot.mybatis.model.SUserDomain;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.LinkedList;

@Mapper
public interface SUserDao {
    LinkedList<SUserDomain> findAll();
    SUserDomain findById(@Param("id") int id);
}
