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