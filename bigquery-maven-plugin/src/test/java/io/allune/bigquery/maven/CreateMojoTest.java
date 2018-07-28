package io.allune.bigquery.maven;

import io.allune.bigquery.maven.service.BigQueryServiceImpl;
import org.apache.maven.plugin.MojoExecutionException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.assertj.core.api.Java6Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class CreateMojoTest {

    @Mock
    private BigQueryServiceImpl bigQueryService;

    @Captor
    private ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);

    @Test
    public void testCreateDataset() throws MojoExecutionException {
        // Given
        CreateMojo mojo = new CreateMojo();
        String expectedDataset = "testDataset";
        mojo.setCreateDataset(true);
        mojo.setDatasetName(expectedDataset);

        mojo.doExecute(bigQueryService);

        Mockito.verify(bigQueryService).createDataSet(captor.capture());
        assertThat(captor.getValue()).isEqualTo(expectedDataset);
    }
}
