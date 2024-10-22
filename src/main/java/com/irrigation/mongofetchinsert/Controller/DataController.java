/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.irrigation.mongofetchinsert.Controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.irrigation.mongofetchinsert.Configuration.MongoConfig;
import com.irrigation.mongofetchinsert.Service.ChunkingService;
import com.irrigation.mongofetchinsert.Service.MongoUtils;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author brune
 */
@RestController
@RequestMapping("/api/data")
@EnableAsync
public class DataController {
    private final MongoUtils mongoUtils;
    private final ControllerHelperService helperService;
    private final String collection;
    private final String sourceCollection;
    
    public DataController(MongoUtils mongoUtils,ControllerHelperService helperService,MongoConfig mongoConfig, ChunkingService chunkingService){
        this.mongoUtils = mongoUtils;
        this.helperService = helperService;
        collection = mongoConfig.MONGO_COLLECTION_AKTY_FINAL;
        sourceCollection = mongoConfig.MONGO_COLLECTION_AKTY_ZNENI;
    }
    
   
    
    @GetMapping("/getAll")
    public ResponseEntity<JsonNode> getAllDocs(
        @RequestHeader("Authorization") String token,  
        @RequestParam(value = "from", required = false) Integer from, 
        @RequestParam(value = "to", required = false) Integer to) {
             if(mongoUtils.isProcessing()){
               return new ResponseEntity<>(HttpStatus.PROCESSING); 
            }

            if(helperService.isTokenValid(token)){
                try {
                  if (from == null) {
                   from = 1;
               }

               if (to == null) {
                   to = mongoUtils.getCollectionSize(collection);
               }
                    JsonNode docs = mongoUtils.getAllWithinRange(collection,from,to);
                    System.out.println(docs.size());
                    System.out.println("Source size" + mongoUtils.getCollectionSize(sourceCollection));
                     return new ResponseEntity<>(docs, HttpStatus.OK);
                } catch (JsonProcessingException ex) {
                    Logger.getLogger(DataController.class.getName()).log(Level.SEVERE, null, ex);
                    return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
                }
            }
            else{
               return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
            }
    }
    
    @GetMapping("/getLinks")
    public ResponseEntity<JsonNode> getAllLinksOnly(@RequestHeader("Authorization") String token) {
             if(mongoUtils.isProcessing()){
               return new ResponseEntity<>(HttpStatus.PROCESSING); 
            }

            if(helperService.isTokenValid(token)){
                try {
                    JsonNode docs = mongoUtils.getLinksFromCollection(collection);
                     return new ResponseEntity<>(docs, HttpStatus.OK);
                } catch (JsonProcessingException ex) {
                    Logger.getLogger(DataController.class.getName()).log(Level.SEVERE, null, ex);
                    return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
                }
            }
            else{
               return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
            }
    }
    
    @GetMapping("/getByLink")
    public ResponseEntity<JsonNode> getOneByLink(@RequestHeader("Authorization") String token, @RequestParam("url")String link) {
            if(mongoUtils.isProcessing()){
               return new ResponseEntity<>(HttpStatus.PROCESSING); 
            }
        
        
            if(helperService.isTokenValid(token)){
                try {
                    JsonNode doc = mongoUtils.getMetadataByLink(collection,link);
                     return new ResponseEntity<>(doc, HttpStatus.OK);
                } catch (JsonProcessingException ex) {
                    Logger.getLogger(DataController.class.getName()).log(Level.SEVERE, null, ex);
                    return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
                } catch (Exception ex) {
                    Logger.getLogger(DataController.class.getName()).log(Level.SEVERE, null, ex);
                    return new ResponseEntity<>(HttpStatus.NOT_FOUND);
                    
                }
            }
            else{
               return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
            }
    }
    
    
         
}
