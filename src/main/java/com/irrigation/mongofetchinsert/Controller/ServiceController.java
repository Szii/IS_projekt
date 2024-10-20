/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.irrigation.mongofetchinsert.Controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.irrigation.mongofetchinsert.Service.MongoSetupService;
import com.irrigation.mongofetchinsert.Service.MongoUtils;
import java.net.MalformedURLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author brune
 */
@RestController
@RequestMapping("/api/service")
@EnableAsync
public class ServiceController {
    private final MongoSetupService mongoSetupService;
    private final MongoUtils mongoUtils;
    private final ControllerHelperService helperService;
    
    public ServiceController(MongoSetupService mongoSetupService,MongoUtils mongoUtils,ControllerHelperService helperService){
        this.mongoSetupService = mongoSetupService;
        this.mongoUtils = mongoUtils;
        this.helperService = helperService;
    }
    
    @PostMapping("/setup")
    public ResponseEntity<JsonNode> setupMongo(@RequestHeader("Authorization") String token) {
             if(mongoUtils.isProcessing()){
               return new ResponseEntity<>(HttpStatus.PROCESSING); 
            }

            if(helperService.isTokenValid(token)){
                 try {
                     mongoSetupService.setupMongo();
                      return new ResponseEntity<>(HttpStatus.OK);
                 } catch (MalformedURLException ex) {
                     Logger.getLogger(ServiceController.class.getName()).log(Level.SEVERE, null, ex);
                     return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
                 }
            }
            else{
               return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
            }
    }
    
}
