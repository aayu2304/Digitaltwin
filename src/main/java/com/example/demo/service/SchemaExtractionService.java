package com.example.demo.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class SchemaExtractionService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public String extractSchema() throws Exception {
        List<Map<String, Object>> tables = jdbcTemplate.queryForList("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'");

        List<Map<String, Object>> schema = new ArrayList<>();

        for (Map<String, Object> table : tables) {
            String tableName = (String) table.get("table_name");
            List<Map<String, Object>> columnsList = jdbcTemplate.queryForList(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?",
                    tableName
            );

            Map<String, Object> columns = new HashMap<>();
            for (Map<String, Object> column : columnsList) {
                String columnName = (String) column.get("column_name");
                String dataType = (String) column.get("data_type");
                Map<String, Object> columnInfo = new HashMap<>();
                columnInfo.put("dataType", dataType);

                String quotedColumnName = "\"" + columnName + "\"";

                if (dataType.equals("integer") || dataType.equals("float") || dataType.equals("double") || dataType.equals("real")) {
                    // Min and Max
                    String minQuery = String.format("SELECT MIN(%s) FROM %s", quotedColumnName, tableName);
                    String maxQuery = String.format("SELECT MAX(%s) FROM %s", quotedColumnName, tableName);
                    Object minValue = jdbcTemplate.queryForObject(minQuery, Object.class);
                    Object maxValue = jdbcTemplate.queryForObject(maxQuery, Object.class);
                    columnInfo.put("minValue", minValue);
                    columnInfo.put("maxValue", maxValue);

                    // Mean
                    String meanQuery = String.format("SELECT AVG(%s) FROM %s", quotedColumnName, tableName);
                    Object meanValue = jdbcTemplate.queryForObject(meanQuery, Object.class);
                    columnInfo.put("meanValue", meanValue);

                    // Median
                    String medianQuery = String.format(
                            "SELECT AVG(%s) FROM (SELECT %s FROM %s ORDER BY %s LIMIT 2 - (SELECT COUNT(*) FROM %s) %% 2 OFFSET (SELECT (COUNT(*) - 1) / 2 FROM %s))",
                            quotedColumnName, quotedColumnName, tableName, quotedColumnName, tableName, tableName
                    );
                    Object medianValue = jdbcTemplate.queryForObject(medianQuery, Object.class);
                    columnInfo.put("medianValue", medianValue);

                    // Mode
                    String modeQuery = String.format(
                            "SELECT %s, COUNT(*) as frequency FROM %s GROUP BY %s ORDER BY frequency DESC LIMIT 1",
                            quotedColumnName, tableName, quotedColumnName
                    );
                    List<Map<String, Object>> modeResult = jdbcTemplate.queryForList(modeQuery);
                    if (!modeResult.isEmpty()) {
                        columnInfo.put("modeValue", modeResult.get(0).get(columnName));
                        columnInfo.put("modeFrequency", modeResult.get(0).get("frequency"));
                    }
                } else if (dataType.equals("varchar") || dataType.equals("character varying")) {
                    // Most Occurring Value and its Frequency
                    String freqQuery = String.format(
                            "SELECT %s, COUNT(*) as frequency FROM %s GROUP BY %s ORDER BY frequency DESC LIMIT 1",
                            quotedColumnName, tableName, quotedColumnName
                    );
                    List<Map<String, Object>> freqResult = jdbcTemplate.queryForList(freqQuery);
                    if (!freqResult.isEmpty()) {
                        columnInfo.put("mostOccurring", freqResult.get(0).get(columnName));
                        columnInfo.put("frequency", freqResult.get(0).get("frequency"));
                    }
                }

                columns.put(columnName, columnInfo);
            }

            Map<String, Object> tableInfo = new HashMap<>();
            tableInfo.put("tname", tableName);
            tableInfo.put("columns", columns);

            schema.add(tableInfo);
        }

        Map<String, Object> schemaMap = new HashMap<>();
        schemaMap.put("tables", schema);

        // Add constraints
        List<Map<String, String>> constraints = new ArrayList<>();
        constraints.add(createConstraint("Supplier", "Batch No", "Production Process Data", "Batch No"));
        constraints.add(createConstraint("Die Information", "Component ID", "Production Process Data", "Component ID"));
        constraints.add(createConstraint("Production Process Data", "Component ID", "Component Quality Measurements", "Component ID"));
        constraints.add(createConstraint("Production Process Data", "Component ID", "Trimming and Hole-Cutting Process Data", "Component ID"));
        constraints.add(createConstraint("Trimming and Hole-Cutting Process Data", "Component ID", "BIW Component Mapping", "Component ID"));
        constraints.add(createConstraint("BIW Component Mapping", "BIW ID", "BIW Weld Data", "BIW ID"));
        constraints.add(createConstraint("Welding Path Data", "BIW ID", "BIW Weld Data", "BIW ID"));
        constraints.add(createConstraint("Welding Path Data", "Path ID", "Welding Deviations", "Path ID"));
        constraints.add(createConstraint("BIW Component Mapping", "Component ID", "Component Defect Details", "Component ID"));
        constraints.add(createConstraint("BIW Component Mapping", "BIW ID", "Sanding Data", "BIW ID"));
        constraints.add(createConstraint("Sanding Data", "BIW ID", "Sealing Data", "BIW ID"));
        constraints.add(createConstraint("Sealing Data", "BIW ID", "Drying Data", "BIW ID"));
        constraints.add(createConstraint("Drying Data", "BIW ID", "Priming Data", "BIW ID"));
        constraints.add(createConstraint("Priming Data", "BIW ID", "Base Coating Data", "BIW ID"));
        constraints.add(createConstraint("Base Coating Data", "BIW ID", "Clear Coating Data", "BIW ID"));
        constraints.add(createConstraint("Clear Coating Data", "BIW ID", "Paint Inspection Results", "BIW ID"));
        constraints.add(createConstraint("Clear Coating Data", "BIW ID", "Door Assembly Data", "BIW ID"));
        constraints.add(createConstraint("Door Assembly Data", "BIW ID", "Dashboard Installation Data", "BIW ID"));
        constraints.add(createConstraint("Dashboard Installation Data", "BIW ID", "Windscreen Assembly Data", "BIW ID"));
        constraints.add(createConstraint("Windscreen Assembly Data", "BIW ID", "Chassis Assembly Data", "BIW ID"));
        constraints.add(createConstraint("Chassis Assembly Data", "BIW ID", "Body Marriage Data", "BIW ID"));
        constraints.add(createConstraint("Body Marriage Data", "BIW ID", "Wheel Assembly Data", "BIW ID"));
        constraints.add(createConstraint("Wheel Assembly Data", "BIW ID", "Seat Assembly", "BIW ID"));
        constraints.add(createConstraint("Seat Assembly", "BIW ID", "Door Reassembly", "BIW ID"));
        constraints.add(createConstraint("Door Reassembly", "BIW ID", "Final Inspection Results", "BIW ID"));
        constraints.add(createConstraint("BIW Component Mapping", "BIW ID", "Sealing Data", "BIW ID"));
        constraints.add(createConstraint("BIW Component Mapping", "BIW ID", "Drying Data", "BIW ID"));
        constraints.add(createConstraint("BIW Component Mapping", "BIW ID", "Priming Data", "BIW ID"));
        constraints.add(createConstraint("BIW Component Mapping", "BIW ID", "Base Coating Data", "BIW ID"));
        constraints.add(createConstraint("BIW Component Mapping", "BIW ID", "Clear Coating Data", "BIW ID"));
        constraints.add(createConstraint("BIW Component Mapping", "BIW ID", "Paint Inspection Results", "BIW ID"));
        constraints.add(createConstraint("BIW Component Mapping", "BIW ID", "Door Assembly Data", "BIW ID"));
        constraints.add(createConstraint("BIW Component Mapping", "BIW ID", "Dashboard Installation Data", "BIW ID"));
        constraints.add(createConstraint("BIW Component Mapping", "BIW ID", "Windscreen Assembly Data", "BIW ID"));
        constraints.add(createConstraint("BIW Component Mapping", "BIW ID", "Chassis Assembly Data", "BIW ID"));
        constraints.add(createConstraint("BIW Component Mapping", "BIW ID", "Body Marriage Data", "BIW ID"));
        constraints.add(createConstraint("BIW Component Mapping", "BIW ID", "Wheel Assembly Data", "BIW ID"));
        constraints.add(createConstraint("BIW Component Mapping", "BIW ID", "Seat Assembly", "BIW ID"));
        constraints.add(createConstraint("BIW Component Mapping", "BIW ID", "Door Reassembly", "BIW ID"));

        schemaMap.put("constraints", constraints);

        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(schemaMap);
    }

    private Map<String, String> createConstraint(String sourceTable, String sourceCol, String targetTable, String targetCol) {
        Map<String, String> constraint = new HashMap<>();
        constraint.put("sourceTable", sourceTable);
        constraint.put("sourceCol", sourceCol);
        constraint.put("targetTable", targetTable);
        constraint.put("targetCol", targetCol);
        return constraint;
    }
}
