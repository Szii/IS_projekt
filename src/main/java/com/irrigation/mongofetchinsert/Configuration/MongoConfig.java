/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.irrigation.mongofetchinsert.Configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;

/**
 *
 * @author brune
 */
@Component
@ConfigurationPropertiesScan
@EnableAsync
public class MongoConfig {
    @Value("${mongo.address}")
    public String ADDRESS;
    @Value("${mongo.database_name}")
    public String DATABASE;
    @Value("${mongo.collectin.pravniAktZneni}")
    public String MONGO_COLLECTION_AKTY_ZNENI;
   @Value("${mongo.collection.pravniAktFinal}")
    public String MONGO_COLLECTION_AKTY_FINAL;
    @Value("${mongo.collection.terminy.base}")
    public String MONGO_COLLECTION_TERMINY_BASE;
    @Value("${mongo.collection.terminy.popis}")
    public String MONGO_COLLECTION_TERMINY_POPIS;
    @Value("${mongo.collection.terminy.vazba}")
    public String MONGO_COLLECTION_TERMINY_VAZBA;
    @Value("${mongo.collection.terminy.final}")
    public String MONGO_COLLECTION_TERMINY_FINAL;
    @Value("${mongo.username}")
    public String USERNAME;
    @Value("${mongo.password}")
    public String PASSWORD;   
}