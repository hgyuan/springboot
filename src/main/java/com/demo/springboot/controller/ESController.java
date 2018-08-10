package com.demo.springboot.controller;



import com.demo.springboot.service.impl.ESServiceImpl;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
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
    ESServiceImpl esService;


    @RequestMapping(method = RequestMethod.POST,value = "/{index}")
    public IndexResponse add(@PathVariable String index){
        return esService.write(index);
    }

    @RequestMapping(method = RequestMethod.DELETE,value = "/{index}")
    public DeleteIndexResponse delete(@PathVariable String index){
        return esService.deleteIndex(index);
    }


    @RequestMapping(method = RequestMethod.POST,value = "/bulk/{index}")
    public void bulk(@PathVariable String index){
        esService.bulkWrite(index);
    }

}
