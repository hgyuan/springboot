package com.demo.springboot.service.impl;


import com.demo.springboot.mybatis.dao.SUserDao;
import com.demo.springboot.mybatis.model.SUserDomain;
import com.demo.springboot.transform.ESTransformer;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;



import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Service
public class ESServiceImpl {
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

    public IndexResponse write(String indexName){
        assert  isIndexExist(indexName);
        SUserDomain userDomain = sUserDao.findById(940);
        IndexRequest request = new IndexRequest(indexName,indexName,String.valueOf(userDomain.getId()));
        Map<String, Object> jsonMap = ESTransformer.transform(userDomain);
        request.source(jsonMap,XContentType.JSON);
        try {
            return restHighLevelClient.index(request);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new IndexResponse();

    }

    public DeleteIndexResponse deleteIndex(String indexName){
        DeleteIndexRequest request = new DeleteIndexRequest(indexName);
        DeleteIndexResponse response = null;
        try {
            response = restHighLevelClient.indices().delete(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }


    public void bulkWrite(String indexName){
        if(!isIndexExist(indexName)){
            System.out.println("index not exists");
        }

        BulkRequest bulkRequest =  new BulkRequest();
        sUserDao.findAll().forEach(user -> {
            IndexRequest indexRequest =
                    new IndexRequest(indexName,indexName,String.valueOf(user.getId()));
            Map<String,Object> jsonMap = ESTransformer.transform(user);
            bulkRequest.add(indexRequest.source(jsonMap,XContentType.JSON));
        });

        try {
            restHighLevelClient.bulk(bulkRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public boolean isIndexExist(String indexName){


        try {
            return restHighLevelClient.indices().exists(new GetIndexRequest().indices(indexName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }


}
