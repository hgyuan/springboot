package com.demo.springboot.controller;



import com.demo.springboot.service.impl.ESService;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/es")
public class ESController {
    @Autowired
    ESService esService;

    @RequestMapping(method = RequestMethod.GET,value="/create/{index}")
    public CreateIndexResponse createIndex(@PathVariable String index){
        return esService.createIndex(index);
    }
    @RequestMapping(method = RequestMethod.GET,value="/add/{index}")
    public IndexResponse add(@PathVariable String index){
        return esService.writeES(index);
    }

    @RequestMapping(method = RequestMethod.GET,value = "/delete/{index}")
    public void delete(@PathVariable String index){
        esService.deleteIndex(index);
    }

}
