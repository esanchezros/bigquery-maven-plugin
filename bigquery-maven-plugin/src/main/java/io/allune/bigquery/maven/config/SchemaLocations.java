/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.allune.bigquery.maven.config;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SchemaLocations {
    private static final Log LOG = LogFactory.getLog(SchemaLocations.class);

    private final List<SchemaLocation> schemaLocationList = new ArrayList<>();

    public SchemaLocations(List<String> rawLocations) {
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

    public List<SchemaLocation> getSchemaLocations() {
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