package com.example.demo.service;



import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class FullExtractionService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public Map<String, Object> extractAllData() {
        List<Map<String, Object>> tables = jdbcTemplate.queryForList(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'");

        Map<String, Object> schemaData = new HashMap<>();

        for (Map<String, Object> table : tables) {
            String tableName = (String) table.get("table_name");
            List<Map<String, Object>> tableData = jdbcTemplate.queryForList("SELECT * FROM " + tableName);
            Map<String, Object> tableWithInsights = new HashMap<>();
            tableWithInsights.put("data", tableData);
            tableWithInsights.put("insights", getTableInsights(tableName, tableData));
            schemaData.put(tableName, tableWithInsights);
        }

        return schemaData;
    }

    private Map<String, Object> getTableInsights(String tableName, List<Map<String, Object>> tableData) {
        Map<String, Object> insights = new HashMap<>();

        // Get column names and types
        List<Map<String, Object>> columns = jdbcTemplate.queryForList(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?", tableName);

        for (Map<String, Object> column : columns) {
            String columnName = (String) column.get("column_name");
            String dataType = (String) column.get("data_type");

            if (dataType.equalsIgnoreCase("integer")) {
                List<Integer> values = tableData.stream()
                        .map(row -> (Integer) row.get(columnName))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                if (!values.isEmpty()) {
                    insights.put(columnName, Map.of(
                            "min", Collections.min(values),
                            "max", Collections.max(values)
                    ));
                }
            }

            // Example for 'color' field, assuming it's a VARCHAR or TEXT field.
            if (dataType.equalsIgnoreCase("character varying") || dataType.equalsIgnoreCase("text")) {
                List<String> values = tableData.stream()
                        .map(row -> (String) row.get(columnName))
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                if (!values.isEmpty()) {
                    String mostFrequentColor = values.stream()
                            .collect(Collectors.groupingBy(color -> color, Collectors.counting()))
                            .entrySet()
                            .stream()
                            .max(Map.Entry.comparingByValue())
                            .map(Map.Entry::getKey)
                            .orElse(null);
                    insights.put(columnName, Map.of(
                            "mostFrequentColor", mostFrequentColor
                    ));
                }
            }
        }

        return insights;
    }
}
