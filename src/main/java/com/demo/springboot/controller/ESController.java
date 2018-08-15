package com.demo.springboot.controller;



import com.demo.springboot.service.impl.ESServiceImpl;
import io.netty.util.internal.StringUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping(value = "/es")
@Api(value="ESController",description = "ElasticSearch Api")
public class ESController {
    @Autowired
    ESServiceImpl esService;


    @ApiOperation(value="创建Index", notes="create Index")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名称", required = true ,dataType = "string")
    })
    @RequestMapping(method = RequestMethod.POST,value = "/{index}")
    public CreateIndexResponse crate(@PathVariable String index){
        return esService.createIndex(index);
    }

    @ApiOperation(value="写Document", notes="write a document in index")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名称", required = true ,dataType = "string")
    })
    @RequestMapping(method = RequestMethod.PUT,value = "/{index}")
    public IndexResponse put(@PathVariable String index){
        return esService.write(index);
    }

    @ApiOperation(value="删除Index", notes="delete Index")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名称", required = true ,dataType = "string")
    })
    @RequestMapping(method = RequestMethod.DELETE,value = "/{index}")
    public DeleteIndexResponse delete(@PathVariable String index){
        return esService.deleteIndex(index);
    }

    @ApiOperation(value="批量写Document", notes="相当于jdbc的batch提交insert语句")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名称", required = true ,dataType = "string")
    })
    @RequestMapping(method = RequestMethod.PUT,value = "/bulk/{index}")
    public void bulk(@PathVariable String index){
        esService.bulkWrite(index);
    }

    @ApiOperation(value="即时分页查询Index", notes="基础分页查询，适应多数场景，此方法ES默认不能超过10000行数据，可配置，但不建议")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名称", required = true ,dataType = "string"),
            @ApiImplicitParam(name = "from", value = "查询起点",dataType = "integer"),
            @ApiImplicitParam(name = "size", value = "单页条数", dataType = "integer")
    })
    @RequestMapping(method = RequestMethod.GET,value = "/{index}")
    public List<Map<String,Object>> search(@PathVariable String index,
                                           @RequestParam(name="from",required = false) Integer from,
                                           @RequestParam(name="size",required = false) Integer size){
        return esService.search(index,from,size);
    }

    @ApiOperation(value="发起滚动逐页查询Index(非即时)", notes="适用大数据量的业务需求，非实时，增删改对于其查询结果不起作用")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名称", required = true ,dataType = "string"),
            @ApiImplicitParam(name = "size", value = "单页条数", required = true ,dataType = "integer")
    })
    @RequestMapping(method = RequestMethod.GET,value = "scroll/{index}")
    public SearchResponse scroll(@PathVariable String index,
                                 @RequestParam(name="size",required = false) Integer size){
        return esService.searchScroll(index,size);
    }

    @ApiOperation(value="滚动逐页查询Index下一页", notes="调用scroll后得到scrollId进行逐行查询")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "scrollId", value = "scroll接口返回的scrollId", required = true ,dataType = "string")
    })
    @RequestMapping(method = RequestMethod.GET,value = "scroll/next/{scrollId}")
    public SearchResponse scrollNext(@PathVariable String scrollId){
        return esService.searchScrollNext(scrollId);
    }
}
