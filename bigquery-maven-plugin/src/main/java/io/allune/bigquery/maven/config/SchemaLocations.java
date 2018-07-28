package io.allune.bigquery.maven.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SchemaLocations {
    private static final Log LOG = LogFactory.getLog(SchemaLocations.class);

    private final List<SchemaLocation> schemaLocationList = new ArrayList<>();

    public SchemaLocations(String... rawLocations) {
        List<SchemaLocation> normalizedSchemaLocations = new ArrayList<>();
        for (String rawLocation : rawLocations) {
            normalizedSchemaLocations.add(new SchemaLocation(rawLocation));
        }
        Collections.sort(normalizedSchemaLocations);

        normalizedSchemaLocations.forEach(normalizedSchemaLocation -> {
            if (schemaLocationList.contains(normalizedSchemaLocation)) {
                LOG.warn("Schema location '" + normalizedSchemaLocation + "' already found, skipping");
                return;
            }
            SchemaLocation parentSchemaLocation = getParentLocationIfExists(normalizedSchemaLocation, schemaLocationList);
            if (parentSchemaLocation != null) {
                LOG.warn("Schema location '" + normalizedSchemaLocation + "' is contained in '" + parentSchemaLocation + "'");
                return;
            }
            schemaLocationList.add(normalizedSchemaLocation);
        });
    }

    public List<SchemaLocation> getSchemaLocation() {
        return schemaLocationList;
    }

    private SchemaLocation getParentLocationIfExists(SchemaLocation schemaLocation, List<SchemaLocation> finalSchemaLocations) {
        for (SchemaLocation finalSchemaLocation : finalSchemaLocations) {
            if (finalSchemaLocation.isParentOf(schemaLocation)) {
                return finalSchemaLocation;
            }
        }
        return null;
    }
}