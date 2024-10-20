/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.irrigation.mongofetchinsert;

import java.util.Collections;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 *
 * @author brune
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableAsync
public class Main {
    public static void main(String[] args){
        SpringApplication app = new SpringApplication(Main.class);
        app.setDefaultProperties(Collections
          .singletonMap("server.port", "9090"));
        app.run(args);
    }
}
