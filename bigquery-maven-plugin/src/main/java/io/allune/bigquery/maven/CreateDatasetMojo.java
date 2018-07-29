package io.allune.bigquery.maven;

import com.google.cloud.bigquery.BigQueryException;
import io.allune.bigquery.maven.service.BigQueryServiceImpl;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;

@Mojo(name = "create-dataset")
public class CreateDatasetMojo extends AbstractBigQueryMojo {

    @Override
    protected void doExecute(BigQueryServiceImpl bigQueryService) throws MojoExecutionException {
        try {
            bigQueryService.createDataset(getDataLocation());
        } catch (BigQueryException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }
}