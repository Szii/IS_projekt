/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.irrigation.mongofetchinsert.Configuration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

/**
 *
 * @author brune
 */

@Component
@EnableAsync
public class MongoConnector {
    
    
    MongoConfig mongoConfig;
    
    public MongoConnector(MongoConfig mongoConfig){
        this.mongoConfig = mongoConfig;
    }
    
    @Bean
    public MongoDatabase getMongoDatabase(){
            MongoClient mongoClient = MongoClients.create(mongoConfig.ADDRESS);
            return mongoClient.getDatabase(mongoConfig.DATABASE);
    }
}
