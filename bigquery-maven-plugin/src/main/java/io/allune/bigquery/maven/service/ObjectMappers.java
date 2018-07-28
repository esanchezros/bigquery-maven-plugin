package io.allune.bigquery.maven.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

final class ObjectMappers {

    private static ObjectMapper objectMapper;

    private ObjectMappers() {
        // no op
    }

    static ObjectMapper mapper() {
        if(objectMapper == null) {
            objectMapper = createObjectMapper();
        }

        return objectMapper;
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(TableFieldSchema.class, new TableFieldSchemaDeserializer());
        objectMapper.registerModule(module);
        objectMapper.registerModule(new Jdk8Module());
        return objectMapper;
    }

    private static class TableFieldSchemaDeserializer extends JsonDeserializer<TableFieldSchema> {

        @Override
        public TableFieldSchema deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            JsonNode node = jp.getCodec().readTree(jp);
            String name = node.get("name").asText();
            String type = node.get("type").asText();
            String mode = node.get("mode").asText();
            List<TableFieldSchema> fields = Lists.newArrayList();
            JsonNode nestedTableFields = node.get("fields");
            if (nestedTableFields != null && nestedTableFields.isArray()) {
                for (JsonNode nestedTableField : nestedTableFields) {
                    TableFieldSchema embedNode = mapper().readValue(nestedTableField.toString(), TableFieldSchema.class);
                    fields.add(embedNode);
                }
            }

            TableFieldSchema tableFieldSchema = new TableFieldSchema();
            tableFieldSchema.setName(name);
            tableFieldSchema.setType(type);
            tableFieldSchema.setMode(mode);
            if (!fields.isEmpty()) {
                tableFieldSchema.setFields(fields);
            }
            return tableFieldSchema;
        }
    }
}