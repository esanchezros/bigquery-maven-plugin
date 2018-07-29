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