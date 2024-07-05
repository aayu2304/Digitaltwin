package com.example.demo.controller;






import com.example.demo.service.SchemaExtractionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SchemaController {

    @Autowired
    private SchemaExtractionService schemaExtractionService;

    @GetMapping("/extract-schema")
    public String extractSchema() {
        try {
            return schemaExtractionService.extractSchema();
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
    }
}