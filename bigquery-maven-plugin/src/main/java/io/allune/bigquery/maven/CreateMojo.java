package io.allune.bigquery.maven;

import com.google.cloud.bigquery.BigQueryException;
import io.allune.bigquery.maven.service.BigQueryServiceImpl;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.ArrayUtils.isNotEmpty;

/**
 * Maven goal to create the BigQuery tables and views for the configured dataset.
 */
@Mojo(name = "create")
public class CreateMojo extends AbstractBigQueryMojo {

    /**
     * Schema locations for native tables.
     * For example: bq/schemas, classpath:bq/schemas, file://etc/bigquery/schemas
     */
    @Parameter(alias = "nativeSchemaLocations", property = "bigquery.nativeSchemaLocations")
    private String[] nativeSchemaLocations;

    /**
     * Schema locations for external tables.
     * For example: bq/schemas, classpath:bq/schemas, file://etc/bigquery/schemas
     */
    @Parameter(alias = "externalSchemaLocations", property = "bigquery.externalSchemaLocations")
    private String[] externalSchemaLocations;

    /**
     * SQL locations for views.
     * For example: bq/schemas, classpath:bq/schemas, file://etc/bigquery/schemas
     */
    @Parameter(alias = "viewLocations", property = "bigquery.viewLocations")
    private String[] viewLocations;

    /**
     * Whether to create the dataset before creating the tables or views
     */
    @Parameter(property = "bigquery.createDataset", defaultValue = "true")
    private boolean createDataset;

    /**
     * Fully-qualified URI that points to your data in Google Cloud Storage
     */
    @Parameter(property = "bigquery.sourceUri", defaultValue = "gs://data.json")
    private String sourceUri;

    /**
     * The source format of the external data
     */
    @Parameter(property = "bigquery.formatOptions", defaultValue = "NEWLINE_DELIMITED_JSON")
    private String formatOptions;

    public void setNativeSchemaLocations(String[] nativeSchemaLocations) {
        this.nativeSchemaLocations = nativeSchemaLocations;
    }

    public void setExternalSchemaLocations(String[] externalSchemaLocations) {
        this.externalSchemaLocations = externalSchemaLocations;
    }

    public void setViewLocations(String[] viewLocations) {
        this.viewLocations = viewLocations;
    }

    public void setCreateDataset(boolean createDataset) {
        this.createDataset = createDataset;
    }

    public void setSourceUri(String sourceUri) {
        this.sourceUri = sourceUri;
    }

    public void setFormatOptions(String formatOptions) {
        this.formatOptions = formatOptions;
    }

    @Override
    protected void doExecute(BigQueryServiceImpl bigQueryService) throws MojoExecutionException {
        try {
            if (createDataset) {
                bigQueryService.createDataset(getDataLocation());
            }

            if (isNotEmpty(nativeSchemaLocations)) {
                bigQueryService.createNativeTables(asList(nativeSchemaLocations));
            }

            if (isNotEmpty(externalSchemaLocations)) {
                bigQueryService.createExternalTables(sourceUri, formatOptions, asList(externalSchemaLocations));
            }

            if (isNotEmpty(viewLocations)) {
                bigQueryService.createViews(asList(viewLocations));
            }
        } catch (ConfigurationException | BigQueryException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }
}