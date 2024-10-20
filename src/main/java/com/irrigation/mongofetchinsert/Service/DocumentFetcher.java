/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.irrigation.mongofetchinsert.Service;

import com.irrigation.mongofetchinsert.Enum.ExtracterType;
import java.net.MalformedURLException;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;

/**
 *
 * @author brune
 */
@Service
@EnableAsync
public  class DocumentFetcher {
    
    
        
    private static final String DATA_AKT_URL = "https://opendata.eselpoint.cz/datove-sady-esbirka/001PravniAktZneni.json.gz";
    private static final String DATA_TERMIN_DEFINICE_URL = "https://opendata.eselpoint.cz/datove-sady-esbirka/031CzechVOCDefiniceTerminu.jsonld.gz";
    private static final String DATA_TERMIN_URL = "https://opendata.eselpoint.cz/datove-sady-esbirka/032CzechVOCTermin.jsonld.gz";
    private static final String DATA_TERMIN_VAZBA_URL = "https://opendata.eselpoint.cz/datove-sady-esbirka/030CzechVOCKoncept.jsonld.gz";
    private static final String DATA_FRAGMENT_URL = "https://opendata.eselpoint.cz/datove-sady-esbirka/003PravniAktZneniFragment.jsonld.gz";
    
    private static final String SOURCE_DATA_COLLECTION_NAME = "PravniAktZneni";
    private static final String SOURCE_TERMINY_COLLECTION_NAME = "TerminyBase";
    private static final String SOURCE_TERMINY_POPIS_COLLECTION_NAME = "TerminyPopis";
    private static final String SOURCE_TERMINY_VAZBA_COLLECTION_NAME = "TerminyVazba";
    private static final String SOURCE_FRAGMENT_COLLECTION_NAME = "Fragment";

    
    private static final String SOURCE_TERMINY_PROCESSED_COLLECTION_NAME = "Terminy";
    private static final String TARGET_COLLECTION_NAME = "PravniAktZneniOdkazyQuick";
    
    private final JsonExtracterUtil jsonExtracter;
    public DocumentFetcher(JsonExtracterUtil jsonExtracter){
        this.jsonExtracter = jsonExtracter;
    }
    
    public  void fetchDocuments() throws MalformedURLException{
          
        jsonExtracter.extractFromAddressToMongo(DATA_AKT_URL,SOURCE_DATA_COLLECTION_NAME,ExtracterType.PRAVNI_AKT);
        jsonExtracter.extractFromAddressToMongo(DATA_TERMIN_DEFINICE_URL, SOURCE_TERMINY_POPIS_COLLECTION_NAME,ExtracterType.TERMIN_DEFINICE);
        jsonExtracter.extractFromAddressToMongo(DATA_TERMIN_URL, SOURCE_TERMINY_COLLECTION_NAME,ExtracterType.TERMIN_NAZEV);
        jsonExtracter.extractFromAddressToMongo(DATA_TERMIN_VAZBA_URL,  SOURCE_TERMINY_VAZBA_COLLECTION_NAME ,ExtracterType.TERMIN_VAZBA);
        //jsonExtracter.extractFromAddressToMongo(DATA_FRAGMENT_URL,  SOURCE_FRAGMENT_COLLECTION_NAME ,ExtracterType.NONE);
    }
    
}
