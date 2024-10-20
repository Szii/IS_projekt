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
    public String address;
    @Value("${mongo.database_name}")
    public String database;
    @Value("${mongo.collection}")
    public String collection;
    @Value("${mongo.username}")
    public String username;
    @Value("${mongo.password}")
    public String password;
    
}
