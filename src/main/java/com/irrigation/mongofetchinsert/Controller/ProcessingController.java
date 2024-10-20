/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.irrigation.mongofetchinsert.Controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.irrigation.mongofetchinsert.Exceptions.UnsupportedFileTypeException;
import com.irrigation.mongofetchinsert.Model.Chunk;
import com.irrigation.mongofetchinsert.Service.ChunkingService;
import com.irrigation.mongofetchinsert.Service.MongoUtils;
import java.io.IOException;
import java.util.List;
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
@RequestMapping("/api/processing")
@EnableAsync
public class ProcessingController {
  private final ChunkingService chunkingService;   
  private final ControllerHelperService helperService;
  private final MongoUtils mongoUtils;

    public ProcessingController(ChunkingService chunkingService, ControllerHelperService helperService, MongoUtils mongoUtils) {
        this.chunkingService = chunkingService;
        this.helperService = helperService;
        this.mongoUtils = mongoUtils;
    }

  
    @GetMapping("/getChunks")
    public ResponseEntity<List<Chunk>> getChunks(@RequestHeader("Authorization") String token, @RequestParam("url")String url) {
            if(mongoUtils.isProcessing()){
               return new ResponseEntity<>(HttpStatus.PROCESSING); 
            }
            if(helperService.isTokenValid(token)){
                List <Chunk> response;
                try {
                    response = chunkingService.processUrl(url);
                } catch (UnsupportedFileTypeException ex) {
                    Logger.getLogger(ProcessingController.class.getName()).log(Level.SEVERE, null, ex);
                     return new ResponseEntity<>(HttpStatus.UNSUPPORTED_MEDIA_TYPE);
                } catch (IOException | InterruptedException ex) {
                    Logger.getLogger(ProcessingController.class.getName()).log(Level.SEVERE, null, ex);
                    return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
                }
                return new ResponseEntity<>(response, HttpStatus.OK);
            }
            else{
               return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
            }
    }
    
    
    
}
