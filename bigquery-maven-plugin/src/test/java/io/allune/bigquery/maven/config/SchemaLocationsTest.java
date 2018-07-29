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

import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.Assert.assertEquals;

public class SchemaLocationsTest {

    @Test
    public void shouldResturnDifferentSchemaLocations() {
        SchemaLocations schemaLocations = new SchemaLocations(asList("dir/schema1", "dir/schema2", "dir/schema3"));
        List<SchemaLocation> schemaLocationList = schemaLocations.getSchemaLocations();
        assertEquals(3, schemaLocationList.size());

        assertThat(schemaLocationList).containsOnly(
                new SchemaLocation("dir/schema1"),
                new SchemaLocation("dir/schema2"),
                new SchemaLocation("dir/schema3"));
    }

    @Test
    public void testSchemaLocationsWithDuplicateLocation() {
        SchemaLocations schemaLocations = new SchemaLocations(asList("dir/schema1", "dir/schema2", "dir/schema2"));
        List<SchemaLocation> schemaLocationList = schemaLocations.getSchemaLocations();
        assertEquals(2, schemaLocationList.size());

        assertThat(schemaLocationList).containsOnly(
                new SchemaLocation("dir/schema1"),
                new SchemaLocation("dir/schema2")
        );
    }

    @Test
    public void shouldRemoveOverlappedSchemaLocations() {
        SchemaLocations schemaLocations = new SchemaLocations(asList("dir/schema2/sub", "dir/schema2", "dir/schema2"));
        List<SchemaLocation> schemaLocationList = schemaLocations.getSchemaLocations();
        assertEquals(1, schemaLocationList.size());
        assertThat(schemaLocationList).containsOnly(
                new SchemaLocation("dir/schema2"));
    }
}