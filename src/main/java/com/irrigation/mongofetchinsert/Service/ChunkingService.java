/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.irrigation.mongofetchinsert.Service;

import com.irrigation.mongofetchinsert.Exceptions.UnsupportedFileTypeException;
import com.irrigation.mongofetchinsert.Model.Chunk;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.ooxml.POIXMLProperties;
import org.apache.poi.ooxml.POIXMLProperties.CoreProperties;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.springframework.stereotype.Service;

/**
 *
 * @author brune
 */

@Service
public class ChunkingService {
   
 public ChunkingService(){
     
 }
    public List<Chunk> processUrl(String url) throws UnsupportedFileTypeException, IOException, InterruptedException{
        List<Chunk> chunks = new ArrayList<>();
          HttpClient httpClient = HttpClient.newHttpClient();
          HttpRequest request = HttpRequest.newBuilder()
              .uri(URI.create(url))
              .build();
          HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());

           InputStream inputStream = response.body();
            BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);

              // Mark the stream to allow resetting after reading the header
              bufferedInputStream.mark(10);

              // Read the first 8 bytes to determine the file type
              byte[] header = new byte[8];
              int bytesRead = bufferedInputStream.read(header, 0, 8);

              // Reset the stream back to the marked position
              bufferedInputStream.reset();

              String fileType = determineFileType(header, bytesRead);

              if ("DOCX".equals(fileType)) {
                 return  processDocx(bufferedInputStream, chunks);
              } else if ("PDF".equals(fileType)) {
                 return  processPdf(bufferedInputStream, chunks);
              }
              else{
                  throw new UnsupportedFileTypeException("file is unsupported");
              }

        }
 
    
    
    
       private  String determineFileType(byte[] header, int bytesRead) {
        if (bytesRead >= 4) {
            String pdfSignature = new String(header, 0, 5, StandardCharsets.US_ASCII);
            if ("%PDF-".equals(pdfSignature)) {
                return "PDF";
            }
            else if (header[0] == (byte) 0x50 && header[1] == (byte) 0x4B && header[2] == (byte) 0x03
                    && header[3] == (byte) 0x04) {
                return "DOCX";
            }
        }
        return null;
    }

 private List<Chunk> processDocx(InputStream inputStream, List<Chunk> chunks) throws IOException {
        // Extract the document title
        XWPFDocument document = new XWPFDocument(inputStream);
  
      System.out.println("DOCX document loaded.");

        // Extract the document title
        String documentTitle = extractDocxTitle(document);
        System.out.println("Extracted document title: " + documentTitle);

        List<XWPFParagraph> paragraphs = document.getParagraphs();
        System.out.println("Total paragraphs: " + paragraphs.size());

        StringBuilder contentBuilder = new StringBuilder();
        String currentSubtitle = null;
        Pattern sectionPattern = Pattern.compile("^§\\s*(\\d+[a-zA-Z]*)");

        for (int i = 0; i < paragraphs.size(); i++) {
            XWPFParagraph para = paragraphs.get(i);
            String text = para.getText().trim();
            System.out.println("Paragraph " + i + ": " + text);

            if (text.isEmpty()) {
                continue; // Skip empty paragraphs
            }

            Matcher matcher = sectionPattern.matcher(text);
            if (matcher.matches()) {
                System.out.println("Found section: " + text);
                // Found a new section
                // Process the previous chunk if any
                if (contentBuilder.length() > 0 && currentSubtitle != null) {
                    String content = contentBuilder.toString().trim();
                    Chunk chunk = new Chunk(documentTitle, currentSubtitle, content);
                    chunks.add(chunk);
                    System.out.println("Added chunk with subtitle: " + currentSubtitle);
                    contentBuilder.setLength(0);
                }

                // Attempt to read the subtitle (paragraph immediately below)
                String subtitle = null;
                if (i + 1 < paragraphs.size()) {
                    String nextText = paragraphs.get(i + 1).getText().trim();
                    System.out.println("Next paragraph for subtitle: " + nextText);
                    if (!nextText.isEmpty() && isSubtitle(nextText)) {
                        subtitle = nextText;
                        System.out.println("Detected subtitle: " + subtitle);
                        i++; // Skip the subtitle paragraph in the next iteration
                    }
                }

                if (subtitle != null && !subtitle.isEmpty()) {
                    currentSubtitle = subtitle;
                } else {
                    // Keep currentSubtitle unchanged
                    if (currentSubtitle == null) {
                        currentSubtitle = "No Subtitle";
                    }
                }

            } else {
                // Accumulate content
                contentBuilder.append(text).append(" ");
            }
        }

        // Process any remaining content
        if (contentBuilder.length() > 0 && currentSubtitle != null) {
            String content = contentBuilder.toString().trim();
            Chunk chunk = new Chunk(documentTitle, currentSubtitle, content);
            chunks.add(chunk);
            System.out.println("Added final chunk with subtitle: " + currentSubtitle);
        }
        System.out.println("Finished processing DOCX.");
        return chunks;
    }


  private List<Chunk> processPdf(InputStream inputStream, List<Chunk> chunks) throws IOException {
        PDDocument document = PDDocument.load(inputStream);
  
       
        // Extract the document title
        String documentTitle = extractPdfTitleFromContent(document);

        PDFTextStripper stripper = new PDFTextStripper();
        String text = stripper.getText(document);

        // Split text into lines
        String[] lines = text.split("\\r?\\n");
        StringBuilder contentBuilder = new StringBuilder();
        String currentSubtitle = null;

        Pattern sectionPattern = Pattern.compile("^§\\s*(\\d+[a-zA-Z]*)");

        for (int i = 0; i < lines.length; i++) {
            String line = lines[i].trim();

            if (line.isEmpty()) {
                continue; // Skip empty lines
            }

            Matcher matcher = sectionPattern.matcher(line);
            if (matcher.matches()) {
                // Found a new section
                // Process the previous chunk if any
                if (contentBuilder.length() > 0 && currentSubtitle != null) {
                    String content = contentBuilder.toString().trim();
                    Chunk chunk = new Chunk(documentTitle, currentSubtitle, content);
                    chunks.add(chunk);
                    contentBuilder.setLength(0);
                }

                // Attempt to read the subtitle (line immediately below)
                String subtitle = null;
                if (i + 1 < lines.length) {
                    String nextLine = lines[i + 1].trim();
                    if (!nextLine.isEmpty() && isSubtitle(nextLine)) {
                        subtitle = nextLine;
                        i++; // Skip the subtitle line in the next iteration
                    }
                }

                if (subtitle != null && !subtitle.isEmpty()) {
                    currentSubtitle = subtitle;
                } else {
                    // Keep currentSubtitle unchanged
                    if (currentSubtitle == null) {
                        currentSubtitle = "No Subtitle";
                    }
                }

            } else {
                // Accumulate content
                contentBuilder.append(line).append(" ");
            }
        }

        // Process any remaining content
        if (contentBuilder.length() > 0 && currentSubtitle != null) {
            String content = contentBuilder.toString().trim();
            Chunk chunk = new Chunk(documentTitle, currentSubtitle, content);
            chunks.add(chunk);
        }
        return chunks;
  }
  
 // Helper method to determine if a line is a subtitle
private static boolean isSubtitle(String text) {
    // Check if the text is in uppercase and of reasonable length
    if (text.equals(text.toUpperCase()) && text.length() > 3 && text.length() < 100) {
        return true;
    }

    // Check if the text matches specific keywords (add your own as needed)
    String[] subtitleKeywords = { "ČÁST", "HLAVA", "DÍL", "ODDÍL", "PŘECHODNÁ USTANOVENÍ", "ZÁVĚREČNÁ USTANOVENÍ",
                                  "Předmět zákona", "Základní pojmy", "Veřejná hydrometeorologická služba" };
    for (String keyword : subtitleKeywords) {
        if (text.equalsIgnoreCase(keyword)) {
            return true;
        }
    }

    return false;
}

private static boolean isSubtitle(XWPFParagraph para) {
    String style = para.getStyle();
    if (style != null) {
        System.out.println("Paragraph style: " + style);
        if (style.matches("Heading[1-6]") || style.equalsIgnoreCase("Subtitle")) {
            return true;
        }
    }

    String text = para.getText().trim();

    // Check if the text is in uppercase and of reasonable length
    if (text.equals(text.toUpperCase()) && text.length() > 3 && text.length() < 100) {
        return true;
    }

    // Check for specific keywords
    String[] subtitleKeywords = { "ČÁST", "HLAVA", "DÍL", "ODDÍL",
        "PŘECHODNÁ USTANOVENÍ", "ZÁVĚREČNÁ USTANOVENÍ",
        "Předmět zákona", "Základní pojmy", "Veřejná hydrometeorologická služba" };
    for (String keyword : subtitleKeywords) {
        if (text.equalsIgnoreCase(keyword)) {
            return true;
        }
    }

    return false;
}


private static String extractPdfTitleFromContent(PDDocument document) throws IOException {
    PDFTextStripper stripper = new PDFTextStripper();
    stripper.setStartPage(1);
    stripper.setEndPage(1);
    String text = stripper.getText(document);

    // Split the text into lines
    String[] lines = text.split("\\r?\\n");

    StringBuilder titleBuilder = new StringBuilder();

    // Collect lines until we reach the first "§" or an empty line
    for (String line : lines) {
        line = line.trim();
        if (line.isEmpty() || line.startsWith("§")) {
            break;
        }
        if (titleBuilder.length() > 0) {
            titleBuilder.append(" ");
        }
        titleBuilder.append(line);
    }

    String documentTitle = titleBuilder.toString().trim();

    if (documentTitle.isEmpty()) {
        documentTitle = "Untitled Document";
    }

    return documentTitle;
}

  
  
  
private static String extractDocxTitle(XWPFDocument document) {
    String documentTitle = null;
    POIXMLProperties props = document.getProperties();
    CoreProperties coreProps = props.getCoreProperties();
    documentTitle = coreProps.getTitle();

    if (documentTitle == null || documentTitle.isEmpty()) {
        // Use the first few paragraphs as the title until an empty paragraph or "§" is encountered
        StringBuilder titleBuilder = new StringBuilder();
        for (XWPFParagraph para : document.getParagraphs()) {
            String text = para.getText().trim();
            if (text.isEmpty() || text.startsWith("§")) {
                break;
            }
            if (titleBuilder.length() > 0) {
                titleBuilder.append(" ");
            }
            titleBuilder.append(text);
        }
        documentTitle = titleBuilder.toString().trim();
    }

    if (documentTitle == null || documentTitle.isEmpty()) {
        documentTitle = "Untitled Document";
    }
    return documentTitle;
}


  
  
  
}
