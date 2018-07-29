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
    private ArgumentCaptor<String> datasetNameCaptor;

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

    @Before
    public void setUp() throws Exception {
        expectedDatasetName = "testDataset";

        String schemaLocation1 = "schemaLocation1";
        String schemaLocation2 = "schemaLocation2";
        expectedSchemaLocations = new String[]{schemaLocation1, schemaLocation2};

        expectedSourceUri = "sourceUri";
        expectedFormatOptions = "formatOptions";
    }

    @Test
    public void testCreateDataset() throws MojoExecutionException {
        // Given
        CreateMojo mojo = new CreateMojo();
        mojo.setCreateDataset(true);
        mojo.setDatasetName(expectedDatasetName);

        // When
        mojo.doExecute(bigQueryService);

        // Then
        verify(bigQueryService).createDataSet(datasetNameCaptor.capture());
        assertThat(datasetNameCaptor.getValue()).isEqualTo(expectedDatasetName);
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
        verify(bigQueryService).createNativeTables(datasetNameCaptor.capture(), schemaListCaptor.capture());
        assertThat(datasetNameCaptor.getValue()).isEqualTo(expectedDatasetName);
        assertThat(schemaListCaptor.getValue()).contains(expectedSchemaLocations);
    }

    @Test
    public void shouldThrowExceptionIfCreateNativeTablesFails() {
        CreateMojo mojo = new CreateMojo();
        mojo.setNativeSchemaLocations(expectedSchemaLocations);
        doThrow(BigQueryException.class).when(bigQueryService).createNativeTables(anyString(), any(List.class));

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
        verify(bigQueryService).createExternalTables(datasetNameCaptor.capture(), sourceUriCaptor.capture(),
                formatOptionsCaptor.capture(), schemaListCaptor.capture());
        assertThat(datasetNameCaptor.getValue()).isEqualTo(expectedDatasetName);
        assertThat(sourceUriCaptor.getValue()).isEqualTo(expectedSourceUri);
        assertThat(formatOptionsCaptor.getValue()).isEqualTo(expectedFormatOptions);
        assertThat(schemaListCaptor.getValue()).contains(expectedSchemaLocations);
    }

    @Test
    public void shouldThrowExceptionIfCreateExternalTablesFails() {
        CreateMojo mojo = new CreateMojo();
        mojo.setExternalSchemaLocations(expectedSchemaLocations);
        doThrow(BigQueryException.class).when(bigQueryService).createExternalTables(anyString(), anyString(),
                anyString(), any(List.class));

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
        verify(bigQueryService).createViews(datasetNameCaptor.capture(), schemaListCaptor.capture());
        assertThat(datasetNameCaptor.getValue()).isEqualTo(expectedDatasetName);
        assertThat(schemaListCaptor.getValue()).contains(expectedSchemaLocations);
    }

    @Test
    public void shouldThrowExceptionIfCreateViewsFails() {
        CreateMojo mojo = new CreateMojo();
        mojo.setViewLocations(expectedSchemaLocations);
        doThrow(BigQueryException.class).when(bigQueryService).createViews(anyString(), any(List.class));

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
