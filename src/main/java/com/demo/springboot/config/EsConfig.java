package com.demo.springboot.config;



import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.io.IOException;


@Configuration
@PropertySource("classpath:config/elasticsearch.properties")
@ConfigurationProperties(prefix = "spring.elasticsearch")
public class EsConfig {
    private String host;
    private int port;
    private String schema;

    private RestHighLevelClient client;

    @Bean
    public RestHighLevelClient client(){
        RestClientBuilder builder = RestClient.builder(new HttpHost(host,port,schema));
        client = new RestHighLevelClient(builder);
        return client;
    }




    public void close(){
        if(client!=null){
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }



    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }


}
