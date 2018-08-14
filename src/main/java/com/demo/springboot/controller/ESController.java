package com.demo.springboot.controller;



import com.demo.springboot.service.impl.ESServiceImpl;
import io.netty.util.internal.StringUtil;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(value = "/es")
public class ESController {
    @Autowired
    ESServiceImpl esService;


    @RequestMapping(method = RequestMethod.POST,value = "/{index}")
    public CreateIndexResponse crate(@PathVariable String index){
        return esService.createIndex(index);
    }
    @RequestMapping(method = RequestMethod.PUT,value = "/{index}")
    public IndexResponse put(@PathVariable String index){
        return esService.write(index);
    }

    @RequestMapping(method = RequestMethod.DELETE,value = "/{index}")
    public DeleteIndexResponse delete(@PathVariable String index){
        return esService.deleteIndex(index);
    }


    @RequestMapping(method = RequestMethod.PUT,value = "/bulk/{index}")
    public void bulk(@PathVariable String index){
        esService.bulkWrite(index);
    }

    @RequestMapping(method = RequestMethod.GET,value = "/{index}")
    public List<Map<String,Object>> search(@PathVariable String index,
                                           @RequestParam(name="from",required = false) Integer from,
                                           @RequestParam(name="size",required = false) Integer size){
        return esService.search(index,from,size);
    }

}
