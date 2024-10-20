/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.irrigation.mongofetchinsert.Service;

import com.irrigation.mongofetchinsert.Model.Segments;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;

/**
 *
 * @author brune
 */
@Service
@EnableAsync
public class SegmentsExtractionUtil {

    /**
     * Extracts the last three segments from a given string separated by '/' and assigns them to two variables:
     * - typSbirky: The third last segment.
     * - cisloAktu: The combination of the second last and last segments separated by '/'.
     *
     * @param input The input string to process.
     * @return A Segments object containing typSbirky and cisloAktu.
     * @throws IllegalArgumentException if the input is invalid or does not contain enough segments.
     */
    public Segments extractSegments(String input) throws IllegalArgumentException {
       
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("Input string must not be null or empty.");
        }

        String trimmedInput = input.trim();
        String[] parts = trimmedInput.split("/");

        if (parts.length < 3) {
            throw new IllegalArgumentException("Input string must contain at least three '/' separated segments.");
        }

        // Extract the last three segments
        String typSbirky = parts[parts.length - 3];
        String rokAktu = parts[parts.length - 2];
        String cisloAktu = parts[parts.length - 1];

        // Combine rokAktu and cisloAktu
        String combinedCisloAktu = rokAktu + "/" + cisloAktu;

        // Validate extracted segments
        if (typSbirky.isEmpty() || rokAktu.isEmpty() || cisloAktu.isEmpty()) {
            throw new IllegalArgumentException("Extracted segments must not be empty.");
        }

        return new Segments(typSbirky, combinedCisloAktu);
        }
    
        
}
