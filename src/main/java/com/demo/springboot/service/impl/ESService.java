package com.demo.springboot.service.impl;


import com.demo.springboot.mybatis.dao.SUserDao;
import com.demo.springboot.mybatis.model.SUserDomain;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Service
public class ESService {
    @Autowired
    RestHighLevelClient restHighLevelClient;
    @Autowired
    SUserDao sUserDao;


    public CreateIndexResponse createIndex(String index){
        CreateIndexRequest request = new CreateIndexRequest(index);
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );
        CreateIndexResponse response = null;
        try {
            response = restHighLevelClient.indices().create(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    public IndexResponse writeES(String indexName){
        assert  isIndexExist(indexName);
        IndexRequest request = new IndexRequest(indexName,indexName);
        SUserDomain userDomain = sUserDao.findById(940);
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("id",userDomain.getId());
        jsonMap.put("userName",userDomain.getUserName());
        jsonMap.put("email",userDomain.getEmail());
        jsonMap.put("enabled",userDomain.getEnabled());
        request.source(jsonMap);
        try {
            return restHighLevelClient.index(request);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new IndexResponse();

    }

    public void deleteIndex(String indexName){
        assert isIndexExist(indexName);
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        try {
            restHighLevelClient.indices().delete(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public boolean isIndexExist(String indexName){
        GetRequest request =  new GetRequest(indexName);
        try {
            return restHighLevelClient.exists(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }


}
