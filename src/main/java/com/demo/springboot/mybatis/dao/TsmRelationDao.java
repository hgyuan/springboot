package com.demo.springboot.mybatis.dao;

import com.demo.springboot.mybatis.model.TsmRelationDomain;
import org.apache.ibatis.annotations.Mapper;

import java.util.LinkedList;

@Mapper
public interface TsmRelationDao {
    public LinkedList<TsmRelationDomain> findAll();
}
