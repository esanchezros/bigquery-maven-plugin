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

import java.util.List;

import static org.assertj.core.api.Java6Assertions.assertThat;
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
    private ArgumentCaptor<String> captor;

    @Captor
    private ArgumentCaptor<List<String>> captorArray;

    @Test
    public void testCreateDataset() throws MojoExecutionException {
        // Given
        CreateMojo mojo = new CreateMojo();
        String expectedDatasetName = "testDataset";
        mojo.setCreateDataset(true);
        mojo.setDatasetName(expectedDatasetName);

        // When
        mojo.doExecute(bigQueryService);

        // Then
        verify(bigQueryService).createDataSet(captor.capture());
        assertThat(captor.getValue()).isEqualTo(expectedDatasetName);
    }

    @Test
    public void shouldThrowExceptionIfCreateDatasetFails() {
        CreateMojo mojo = new CreateMojo();
        mojo.setCreateDataset(true);
        doThrow(BigQueryException.class).when(bigQueryService).createDataSet(anyString());

        // When
        try {
            mojo.doExecute(bigQueryService);
            fail("Expected MojoExecutionException");
        } // Then
        catch (MojoExecutionException ex) {
            // Expected
        }
    }

    @Test
    public void testCreateNativeTables() throws MojoExecutionException {
        // Given
        CreateMojo mojo = new CreateMojo();
        String expectedDatasetName = "testDataset";
        mojo.setDatasetName(expectedDatasetName);
        String schemaLocation1 = "schemaLocation1";
        String schemaLocation2 = "schemaLocation2";
        String[] expectedNativeSchemaLocations = {schemaLocation1, schemaLocation2};
        mojo.setNativeSchemaLocations(expectedNativeSchemaLocations);

        // When
        mojo.doExecute(bigQueryService);

        // Then
        verify(bigQueryService).createNativeTables(captor.capture(), captorArray.capture());
        assertThat(captor.getValue()).isEqualTo(expectedDatasetName);
        assertThat(captorArray.getValue()).contains(expectedNativeSchemaLocations);
    }

    @Test
    public void shouldThrowExceptionIfCreateNativeTablesFails() {
        CreateMojo mojo = new CreateMojo();
        String schemaLocation1 = "schemaLocation1";
        String schemaLocation2 = "schemaLocation2";
        String[] expectedNativeSchemaLocations = {schemaLocation1, schemaLocation2};
        mojo.setNativeSchemaLocations(expectedNativeSchemaLocations);
        doThrow(BigQueryException.class).when(bigQueryService).createNativeTables(anyString(), any(List.class));

        // When
        try {
            mojo.doExecute(bigQueryService);
            fail("Expected MojoExecutionException");
        } // Then
        catch (MojoExecutionException ex) {
            // Expected
        }
    }
}
