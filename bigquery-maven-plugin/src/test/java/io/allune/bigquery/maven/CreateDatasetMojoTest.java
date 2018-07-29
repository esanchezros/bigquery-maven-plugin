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
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class CreateDatasetMojoTest {

    @Mock
    private BigQueryServiceImpl bigQueryService;

    @Captor
    private ArgumentCaptor<String> dataLocationCaptor;

    @Test
    public void testCreateDataset() throws MojoExecutionException {
        // Given
        CreateDatasetMojo mojo = new CreateDatasetMojo();
        String expectedDataLocation = "testDataLocation";
        mojo.setDataLocation(expectedDataLocation);

        // When
        mojo.doExecute(bigQueryService);

        // Then
        verify(bigQueryService).createDataset(dataLocationCaptor.capture());
        assertThat(dataLocationCaptor.getValue()).isEqualTo(expectedDataLocation);
    }

    @Test
    public void testCreateDatasetShouldThrowMojoExecutionException() {
        // Given
        CreateDatasetMojo mojo = new CreateDatasetMojo();
        doThrow(BigQueryException.class).when(bigQueryService).createDataset(anyString());

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