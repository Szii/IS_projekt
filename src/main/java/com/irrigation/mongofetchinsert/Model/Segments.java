package com.irrigation.mongofetchinsert.Model;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author brune
 */
public class Segments {
    private final String typSbirky;
    private final String cisloAktu;

    /**
     * Constructs a Segments object with the specified typSbirky and cisloAktu.
     *
     * @param typSbirky The type of sbirka (e.g., "sb").
     * @param cisloAktu  The combination of rokAktu and cisloAktu (e.g., "1995/104").
     */
    public Segments(String typSbirky, String cisloAktu) {
        if (typSbirky == null || typSbirky.trim().isEmpty()) {
            throw new IllegalArgumentException("typSbirky cannot be null or empty.");
        }
        if (cisloAktu == null || cisloAktu.trim().isEmpty()) {
            throw new IllegalArgumentException("cisloAktu cannot be null or empty.");
        }
        this.typSbirky = typSbirky;
        this.cisloAktu = cisloAktu;
    }

    public String getTypSbirky() {
        return typSbirky;
    }

    public String getCisloAktu() {
        return cisloAktu;
    }

    @Override
    public String toString() {
        return "Segments{" +
                "typSbirky='" + typSbirky + '\'' +
                ", cisloAktu='" + cisloAktu + '\'' +
                '}';
    }

    // Optionally, override equals() and hashCode() if needed for collections or comparisons.
}
