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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class CleanMojoTest {

    @Mock
    private BigQueryServiceImpl bigQueryService;

    @Captor
    private ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);

    @Test
    public void testShouldCleanTables() throws MojoExecutionException {

        // Given
        CleanMojo mojo = new CleanMojo();
        String expectedDataset = "testDataset";
        mojo.setDatasetName(expectedDataset);
        mojo.setDeleteTables(true);

        // When
        mojo.doExecute(bigQueryService);

        // Then
        verify(bigQueryService).deleteTables(captor.capture());
        assertThat(captor.getValue(), is(expectedDataset));
        verify(bigQueryService, never()).deleteDataset(anyString(), eq(false));
    }

    @Test
    public void testShouldCleanDatasetIfDeleteDatasetIsTrue() throws MojoExecutionException {

        // Given
        CleanMojo mojo = new CleanMojo();
        String expectedDataset = "testDataset";
        mojo.setDatasetName(expectedDataset);
        mojo.setDeleteDataset(true);

        // When
        mojo.doExecute(bigQueryService);

        // Then
        verify(bigQueryService).deleteDataset(captor.capture(), eq(false));
        assertThat(captor.getValue(), is(expectedDataset));
    }

    @Test
    public void testWhenDeleteTableFailsShouldThrowMojoExecutionException() {
        // Given
        CleanMojo mojo = new CleanMojo();
        mojo.setDeleteTables(true);
        doThrow(BigQueryException.class).when(bigQueryService).deleteTables(anyString());

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