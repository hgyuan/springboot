package com.demo.springboot.service.impl;



import com.demo.springboot.mybatis.dao.SUserDao;
import com.demo.springboot.mybatis.model.SUserDomain;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.LinkedList;

@Service
public class UserService {
    @Autowired
    SUserDao sUserDao;
    public LinkedList<SUserDomain> findAll(){
        return sUserDao.findAll();
    }

    public SUserDomain findById(int id) {
        return sUserDao.findById(id);
    }


}
