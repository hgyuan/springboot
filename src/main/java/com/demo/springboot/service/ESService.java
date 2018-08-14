package com.demo.springboot.service;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.index.IndexResponse;

import java.util.List;
import java.util.Map;

public interface ESService {

    public CreateIndexResponse create(String indexName);
    public IndexResponse write(String indexName,Map<String,Object> doc);
    public List<Map<String,Object>> search(String indexName, Integer from , Integer to);
    public DeleteIndexResponse deleteIndex(String indexName);



}
