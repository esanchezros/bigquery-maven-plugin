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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class CleanMojoTest {

    @Mock
    private BigQueryServiceImpl bigQueryService;

    @Test
    public void testDeleteTables() throws MojoExecutionException {

        // Given
        CleanMojo mojo = new CleanMojo();
        String expectedDataset = "testDataset";
        mojo.setDatasetName(expectedDataset);
        mojo.setDeleteTables(true);

        // When
        mojo.doExecute(bigQueryService);

        // Then
        verify(bigQueryService).deleteTables();
        verify(bigQueryService, never()).deleteDataset(eq(false));
    }

    @Test
    public void shouldThrowExceptionIfDeleteTablesFails() {
        // Given
        CleanMojo mojo = new CleanMojo();
        mojo.setDeleteTables(true);
        doThrow(BigQueryException.class).when(bigQueryService).deleteTables();

        try {
            // When
            mojo.doExecute(bigQueryService);
            fail("MojoExecutionException expected");
        } // Then
        catch (MojoExecutionException ex) {
            // Expected
            assertTrue(ex.getCause() instanceof BigQueryException);
        }
    }

    @Test
    public void testDeleteDataset() throws MojoExecutionException {

        // Given
        CleanMojo mojo = new CleanMojo();
        String expectedDataset = "testDataset";
        mojo.setDatasetName(expectedDataset);
        mojo.setDeleteDataset(true);

        // When
        mojo.doExecute(bigQueryService);

        // Then
        verify(bigQueryService).deleteDataset(eq(false));
    }

    @Test
    public void shouldThrowExceptionIfDeleteDatasetFails() {
        // Given
        CleanMojo mojo = new CleanMojo();
        mojo.setDeleteDataset(true);
        doThrow(BigQueryException.class).when(bigQueryService).deleteDataset(eq(false));

        try {
            // When
            mojo.doExecute(bigQueryService);
            fail("MojoExecutionException expected");
        } // Then
        catch (MojoExecutionException ex) {
            // Expected
            assertTrue(ex.getCause() instanceof BigQueryException);
        }
    }

    @Test
    public void testDeleteDatasetForced() throws MojoExecutionException {

        // Given
        CleanMojo mojo = new CleanMojo();
        String expectedDataset = "testDataset";
        mojo.setDatasetName(expectedDataset);
        mojo.setDeleteDataset(true);
        mojo.setForceDeleteDataset(true);

        // When
        mojo.doExecute(bigQueryService);

        // Then
        verify(bigQueryService).deleteDataset(eq(true));
    }

    @Test
    public void shouldThrowExceptionIfForcedDeleteDatasetFails() {
        // Given
        CleanMojo mojo = new CleanMojo();
        mojo.setDeleteDataset(true);
        mojo.setForceDeleteDataset(true);
        doThrow(BigQueryException.class).when(bigQueryService).deleteDataset(eq(true));

        try {
            // When
            mojo.doExecute(bigQueryService);
            fail("MojoExecutionException expected");
        } // Then
        catch (MojoExecutionException ex) {
            // Expected
            assertTrue(ex.getCause() instanceof BigQueryException);
        }
    }
}