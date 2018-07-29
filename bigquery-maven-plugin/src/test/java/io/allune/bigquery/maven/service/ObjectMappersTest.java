package io.allune.bigquery.maven.service;

import com.fasterxml.jackson.databind.type.CollectionType;
import com.google.api.services.bigquery.model.TableFieldSchema;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static io.allune.bigquery.maven.service.ObjectMappers.mapper;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class ObjectMappersTest {

    private static final String TABLE_FIELD_SCHEMA = "[\n" +
            "  {\n" +
            "    \"name\": \"id\",\n" +
            "    \"mode\": \"NULLABLE\",\n" +
            "    \"type\": \"STRING\"\n" +
            "  },\n" +
            "  {\n" +
            "    \"name\": \"fields\",\n" +
            "    \"type\": \"RECORD\",\n" +
            "    \"mode\": \"REPEATED\",\n" +
            "    \"fields\": [\n" +
            "      {\n" +
            "        \"name\": \"field1\",\n" +
            "        \"type\": \"STRING\",\n" +
            "        \"mode\": \"REQUIRED\"\n" +
            "      },\n" +
            "      {\n" +
            "        \"name\": \"moreFields\",\n" +
            "        \"type\": \"RECORD\",\n" +
            "        \"mode\": \"REPEATED\",\n" +
            "        \"fields\": [\n" +
            "          {\n" +
            "            \"name\": \"field1\",\n" +
            "            \"type\": \"STRING\",\n" +
            "            \"mode\": \"REQUIRED\"\n" +
            "          },\n" +
            "          {\n" +
            "            \"name\": \"field2\",\n" +
            "            \"type\": \"STRING\",\n" +
            "            \"mode\": \"REQUIRED\"\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "]";

    @Test
    public void shouldDeserialiseNestedTableFieldSchema() throws IOException {

        CollectionType type = mapper().getTypeFactory().constructCollectionType(List.class, TableFieldSchema.class);
        List<TableFieldSchema> fieldSchemas = ObjectMappers.mapper().readValue(TABLE_FIELD_SCHEMA, type);
        assertThat(fieldSchemas).isNotNull();

        TableFieldSchema expectedTableFieldSchema = new TableFieldSchema();
        expectedTableFieldSchema.setName("id");
        expectedTableFieldSchema.setType("STRING");
        expectedTableFieldSchema.setMode("NULLABLE");
        assertThat(fieldSchemas).contains(expectedTableFieldSchema);
    }
}