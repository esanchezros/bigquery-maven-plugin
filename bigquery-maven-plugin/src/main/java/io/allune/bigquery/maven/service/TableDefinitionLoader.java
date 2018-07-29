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

package io.allune.bigquery.maven.service;

import com.fasterxml.jackson.databind.type.CollectionType;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.ViewDefinition;
import io.allune.bigquery.maven.ConfigurationException;
import org.apache.commons.io.IOUtils;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static io.allune.bigquery.maven.service.ObjectMappers.mapper;
import static java.util.stream.Collectors.toList;

class TableDefinitionLoader {

    private TableDefinitionLoader() {
        // no op
    }

    static TableDefinition loadStandardTableDefinition(Resource resource) {
        return StandardTableDefinition.of(loadTableSchema(resource));
    }

    static TableDefinition loadExternalTableDefinition(String sourceUri, String formatOptions, Resource resource) {
        return ExternalTableDefinition.of(sourceUri, loadTableSchema(resource), FormatOptions.of(formatOptions));
    }

    static TableDefinition loadViewDefinition(Resource resource, String projectId, String dataset) {
        try {
            String view = IOUtils.toString(resource.getInputStream(), "UTF-8")
                    .replace("$projectId", projectId)
                    .replace("$datasetName", dataset);
            return ViewDefinition.of(view);
        } catch (IOException e) {
            throw new ConfigurationException(e.getMessage(), e);
        }
    }

    private static Schema loadTableSchema(Resource resource) {
        List<TableFieldSchema> fieldSchemas = getTableFieldSchemas(resource);
        List<TableFieldSchema> tableFieldSchemas = createTableSchema(fieldSchemas);
        TableSchema sourceSchema = new TableSchema();
        sourceSchema.setFields(tableFieldSchemas);
        return fromPb(sourceSchema);
    }

    private static List<TableFieldSchema> getTableFieldSchemas(Resource resource) {
        List<TableFieldSchema> fieldSchemas;
        try {
            CollectionType type = mapper().getTypeFactory().constructCollectionType(List.class, TableFieldSchema.class);
            fieldSchemas = mapper().readValue(resource.getInputStream(), type);
        } catch (IOException e) {
            throw new ConfigurationException(e.getMessage(), e);
        }
        return fieldSchemas;
    }

    private static List<TableFieldSchema> createTableSchema(List<TableFieldSchema> tableFieldSchemas) {

        List<TableFieldSchema> allFields = newArrayList();
        List<TableFieldSchema> simpleFields = tableFieldSchemas.stream().filter(tfs -> !tfs.getType().equals("RECORD")).collect(toList());
        allFields.addAll(simpleFields);

        List<TableFieldSchema> recordFields = tableFieldSchemas.stream().filter(tfs -> tfs.getType().equals("RECORD")).collect(toList());
        recordFields.forEach(record -> {
            record.setFields(createTableSchema(record.getFields()));
            allFields.add(record);
        });

        return allFields;
    }

    private static Schema fromPb(TableSchema tableSchemaPb) {
        try {
            Method fromPb = Schema.class.getDeclaredMethod("fromPb", TableSchema.class);
            fromPb.setAccessible(true);
            return (Schema) fromPb.invoke(null, tableSchemaPb);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new ConfigurationException(e.getMessage(), e);
        }
    }
}
