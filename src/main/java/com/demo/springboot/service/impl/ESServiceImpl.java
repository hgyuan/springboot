package com.demo.springboot.service.impl;


import com.demo.springboot.mybatis.dao.SUserDao;
import com.demo.springboot.mybatis.dao.TsmRelationDao;
import com.demo.springboot.mybatis.model.SUserDomain;
import com.demo.springboot.transform.ESTransformer;
import com.sun.org.apache.xerces.internal.xs.datatypes.ObjectList;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;



import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.QuerySearchRequest;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Service
public class ESServiceImpl {
    @Autowired
    RestHighLevelClient restHighLevelClient;
    @Autowired
    SUserDao sUserDao;
    @Autowired
    TsmRelationDao tsmRelationDao;


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
        SUserDomain userDomain = sUserDao.findById(940);
        IndexRequest request = new IndexRequest(indexName,indexName,String.valueOf(userDomain.getId()));
        Map<String, Object> jsonMap = ESTransformer.transform2Map(userDomain);
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
        BulkRequest bulkRequest =  new BulkRequest();
//        sUserDao.findAll().forEach(user -> {
//            IndexRequest indexRequest =
//                    new IndexRequest(indexName,indexName,String.valueOf(user.getId()));
//            Map<String,Object> jsonMap = ESTransformer.transform2Map(user);
//            bulkRequest.add(indexRequest.source(jsonMap,XContentType.JSON));
//        });
        tsmRelationDao.findAll().forEach(user -> {
            IndexRequest indexRequest =
                    new IndexRequest(indexName,indexName,String.valueOf(user.getId()));
            Map<String,Object> jsonMap = ESTransformer.transform2Map(user);
            bulkRequest.add(indexRequest.source(jsonMap,XContentType.JSON));
        });

        try {
            restHighLevelClient.bulk(bulkRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public List<Map<String,Object>> search(String indexName,Integer from,Integer size){
        if(!isIndexExist(indexName)){
            return null;
        }
        SearchResponse searchResponse = null;
        SearchRequest searchRequest = new SearchRequest().indices(indexName);
        FieldSortBuilder fieldSortBuilder = new FieldSortBuilder("id").order(SortOrder.ASC);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(from==null?0:from);
        searchSourceBuilder.size(size==null?10:size);
        searchSourceBuilder.sort(fieldSortBuilder);

        searchRequest.source(searchSourceBuilder);
        try {
            searchResponse = restHighLevelClient.search(searchRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        Arrays.asList(searchResponse.getHits().getHits()).stream().
                forEach(hit -> list.add(hit.getSourceAsMap()));
        return list;
    }

    public void searchScroll(String indexName){

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
