package com.example.demo.controller;



import com.example.demo.service.FullExtractionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class FullExtractionController {

    @Autowired
    private FullExtractionService dataExtractionService;

    @GetMapping("/extract-all-data")
    public Map<String, Object> extractAllData() {
        return dataExtractionService.extractAllData();
    }
}
