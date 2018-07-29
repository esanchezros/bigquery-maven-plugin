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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SchemaLocationTest {

    @Test
    public void shouldApplyClasspathPrefix() {
        SchemaLocation schemaLocation = new SchemaLocation("dir/schemas");
        assertEquals("classpath:", schemaLocation.getPrefix());
        assertTrue(schemaLocation.isClassPath());
        assertEquals("dir/schemas", schemaLocation.getPath());
        assertEquals("classpath:dir/schemas", schemaLocation.getDescriptor());
    }

    @Test
    public void shouldCreateClasspathSchemaLocation() {
        SchemaLocation schemaLocation = new SchemaLocation("classpath:dir/schemas");
        assertEquals("classpath:", schemaLocation.getPrefix());
        assertTrue(schemaLocation.isClassPath());
        assertEquals("dir/schemas", schemaLocation.getPath());
        assertEquals("classpath:dir/schemas", schemaLocation.getDescriptor());
    }

    @Test
    public void shouldCreateFilesystemLocation() {
        SchemaLocation schemaLocation = new SchemaLocation("file:dir/schemas");
        assertEquals("file:", schemaLocation.getPrefix());
        assertFalse(schemaLocation.isClassPath());
        assertEquals("dir/schemas", schemaLocation.getPath());
        assertEquals("file:dir/schemas", schemaLocation.getDescriptor());
    }
}