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

package io.allune.bigquery.maven;

import com.google.cloud.bigquery.BigQueryException;
import io.allune.bigquery.maven.service.BigQueryServiceImpl;
import org.apache.maven.plugin.MojoExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class CreateMojoTest {

    @Mock
    private BigQueryServiceImpl bigQueryService;

    @Captor
    private ArgumentCaptor<String> dataLocationCaptor;

    @Captor
    private ArgumentCaptor<List<String>> schemaListCaptor;

    @Captor
    private ArgumentCaptor<String> sourceUriCaptor;

    @Captor
    private ArgumentCaptor<String> formatOptionsCaptor;

    private String expectedDatasetName;
    private String[] expectedSchemaLocations;
    private String expectedSourceUri;
    private String expectedFormatOptions;
    private String expectedDataLocation;

    @Before
    public void setUp() {
        expectedDatasetName = "testDataset";

        String schemaLocation1 = "schemaLocation1";
        String schemaLocation2 = "schemaLocation2";
        expectedSchemaLocations = new String[]{schemaLocation1, schemaLocation2};

        expectedSourceUri = "sourceUri";
        expectedFormatOptions = "formatOptions";
        expectedDataLocation = "dataLocation";
    }

    @Test
    public void testCreateDataset() throws MojoExecutionException {
        // Given
        CreateMojo mojo = new CreateMojo();
        mojo.setCreateDataset(true);
        mojo.setDatasetName(expectedDatasetName);
        mojo.setDataLocation(expectedDataLocation);

        // When
        mojo.doExecute(bigQueryService);

        // Then
        verify(bigQueryService).createDataset(dataLocationCaptor.capture());
        assertThat(dataLocationCaptor.getValue()).isEqualTo(expectedDataLocation);
    }

    @Test
    public void shouldThrowExceptionIfCreateDatasetFails() {
        CreateMojo mojo = new CreateMojo();
        mojo.setCreateDataset(true);
        doThrow(BigQueryException.class).when(bigQueryService).createDataset(anyString());

        // When
        try {
            mojo.doExecute(bigQueryService);
            fail("Expected MojoExecutionException");
        } // Then
        catch (MojoExecutionException ex) {
            // Expected
            assertTrue(ex.getCause() instanceof BigQueryException);
        }
    }

    @Test
    public void testCreateNativeTables() throws MojoExecutionException {
        // Given
        CreateMojo mojo = new CreateMojo();
        mojo.setDatasetName(expectedDatasetName);
        mojo.setNativeSchemaLocations(expectedSchemaLocations);

        // When
        mojo.doExecute(bigQueryService);

        // Then
        verify(bigQueryService).createNativeTables(schemaListCaptor.capture());
        assertThat(schemaListCaptor.getValue()).contains(expectedSchemaLocations);
    }

    @Test
    public void shouldThrowExceptionIfCreateNativeTablesFails() {
        CreateMojo mojo = new CreateMojo();
        mojo.setNativeSchemaLocations(expectedSchemaLocations);
        doThrow(BigQueryException.class).when(bigQueryService).createNativeTables(any(List.class));

        // When
        try {
            mojo.doExecute(bigQueryService);
            fail("Expected MojoExecutionException");
        } // Then
        catch (MojoExecutionException ex) {
            // Expected
            assertTrue(ex.getCause() instanceof BigQueryException);
        }
    }

    @Test
    public void testCreateExternalTables() throws MojoExecutionException {
        // Given
        CreateMojo mojo = new CreateMojo();
        mojo.setDatasetName(expectedDatasetName);
        mojo.setExternalSchemaLocations(expectedSchemaLocations);
        mojo.setSourceUri(expectedSourceUri);
        mojo.setFormatOptions(expectedFormatOptions);

        // When
        mojo.doExecute(bigQueryService);

        // Then
        verify(bigQueryService).createExternalTables(sourceUriCaptor.capture(), formatOptionsCaptor.capture(),
                schemaListCaptor.capture());
        assertThat(sourceUriCaptor.getValue()).isEqualTo(expectedSourceUri);
        assertThat(formatOptionsCaptor.getValue()).isEqualTo(expectedFormatOptions);
        assertThat(schemaListCaptor.getValue()).contains(expectedSchemaLocations);
    }

    @Test
    public void shouldThrowExceptionIfCreateExternalTablesFails() {
        CreateMojo mojo = new CreateMojo();
        mojo.setExternalSchemaLocations(expectedSchemaLocations);
        doThrow(BigQueryException.class).when(bigQueryService).createExternalTables(anyString(), anyString(), any(List.class));

        // When
        try {
            mojo.doExecute(bigQueryService);
            fail("Expected MojoExecutionException");
        } // Then
        catch (MojoExecutionException ex) {
            // Expected
            assertTrue(ex.getCause() instanceof BigQueryException);
        }
    }

    @Test
    public void testCreateViews() throws MojoExecutionException {
        // Given
        CreateMojo mojo = new CreateMojo();
        mojo.setDatasetName(expectedDatasetName);
        mojo.setViewLocations(expectedSchemaLocations);

        // When
        mojo.doExecute(bigQueryService);

        // Then
        verify(bigQueryService).createViews(schemaListCaptor.capture());
        assertThat(schemaListCaptor.getValue()).contains(expectedSchemaLocations);
    }

    @Test
    public void shouldThrowExceptionIfCreateViewsFails() {
        CreateMojo mojo = new CreateMojo();
        mojo.setViewLocations(expectedSchemaLocations);
        doThrow(BigQueryException.class).when(bigQueryService).createViews(any(List.class));

        // When
        try {
            mojo.doExecute(bigQueryService);
            fail("Expected MojoExecutionException");
        } // Then
        catch (MojoExecutionException ex) {
            // Expected
            assertTrue(ex.getCause() instanceof BigQueryException);
        }
    }
}
