/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.irrigation.mongofetchinsert.Enum;

/**
 *
 * @author brune
 */
public class VztazenyTermin {
    String vztazenyTerminNazev;
    String vztazenyTerminText;

    public VztazenyTermin(String vztazenyTerminNazev, String vztazenyTerminText) {
        this.vztazenyTerminNazev = vztazenyTerminNazev;
        this.vztazenyTerminText = vztazenyTerminText;
    }
    
    

    public String getVztazenyTerminNazev() {
        return vztazenyTerminNazev;
    }

    public void setVztazenyTerminNazev(String vztazenyTerminNazev) {
        this.vztazenyTerminNazev = vztazenyTerminNazev;
    }

    public String getVztazenyTerminText() {
        return vztazenyTerminText;
    }

    public void setVztazenyTerminText(String vztazenyTerminText) {
        this.vztazenyTerminText = vztazenyTerminText;
    }
    
}
