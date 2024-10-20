/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.irrigation.mongofetchinsert.Controller;

import com.irrigation.mongofetchinsert.Configuration.ApiConfig;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;

/**
 *
 * @author brune
 */
@Service
@EnableAsync
public class ControllerHelperService {
    
    String token;
    

    
    ControllerHelperService(ApiConfig apiConfig){
        token = apiConfig.apiKey;
    }
      
    private String getToken(String token){
        if (token.startsWith("Bearer ")) {
           token = token.substring(7);
       }
        return token;
    }
    
    
    public boolean isTokenValid(String givenHeaderAuthParameter){       
        return getToken(givenHeaderAuthParameter).equals(token);      
    }
    
       
}
