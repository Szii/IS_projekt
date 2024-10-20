/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.irrigation.mongofetchinsert.Service;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;

/**
 *
 * @author brune
 */
@Service
@EnableAsync
public class StringParser {

    /**
     * Extracts the integer ID from the end of a string formatted as "something/id".
     *
     * @param input The input string containing the ID after the last slash.
     * @return The extracted ID as an integer.
     * @throws IllegalArgumentException If the input string does not contain a slash.
     * @throws NumberFormatException    If the substring after the last slash is not a valid integer.
     */
    public int extractIdAfterLastSlashAsInt(String input) throws IllegalArgumentException {
        if (input == null || input.isEmpty()) {
            throw new IllegalArgumentException("Input string cannot be null or empty.");
        }

        int lastSlashIndex = input.lastIndexOf('/');

        if (lastSlashIndex == -1 || lastSlashIndex == input.length() - 1) {
            throw new IllegalArgumentException("Input string must contain a '/' followed by an integer ID.");
        }

        String idStr = input.substring(lastSlashIndex + 1).trim();

        try {
            return Integer.parseInt(idStr);
        } catch (NumberFormatException e) {
            throw new NumberFormatException("The substring after the last '/' is not a valid integer: '" + idStr + "'");
        }
    }
    
    
      public String extractAfterLastSlashAsString(String input) {
        if (input == null || !input.contains("/")) {
            return input;
        }
        return input.substring(input.lastIndexOf('/') + 1);
    }
    
    public String extractFormattedText(String input) {
         if (input == null || input.isEmpty()) {
            return null;  // Return null if the input is empty or null
        }

        // Regular expression to capture the text between `>` and `</div>`
        Pattern pattern = Pattern.compile(">([^<]+)</div>");
        Matcher matcher = pattern.matcher(input);

        if (matcher.find()) {
            return matcher.group(1).trim();  // Return the extracted text, trimmed of any surrounding spaces
        }

        // Return null if no match is found
        return null;
    }
    

}
