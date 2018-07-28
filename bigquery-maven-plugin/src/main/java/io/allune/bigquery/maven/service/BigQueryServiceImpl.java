package io.allune.bigquery.maven.service;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetDeleteOption;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import io.allune.bigquery.maven.ConfigurationException;
import io.allune.bigquery.maven.config.SchemaLocations;
import org.apache.commons.io.FilenameUtils;
import org.apache.maven.plugin.logging.Log;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.auth.oauth2.GoogleCredentials.getApplicationDefault;
import static com.google.auth.oauth2.ServiceAccountCredentials.fromStream;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.allune.bigquery.maven.service.TableDefinitionLoader.loadExternalTableDefinition;
import static io.allune.bigquery.maven.service.TableDefinitionLoader.loadStandardTableDefinition;
import static io.allune.bigquery.maven.service.TableDefinitionLoader.loadViewDefinition;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.asList;

public class BigQueryServiceImpl implements BigQueryService {

    private final BigQuery bigQuery;
    private final String projectId;
    private final String dataset;
    private final String dataLocation;
    private final Log log;

    private BigQueryServiceImpl(BigQuery bigQuery, String projectId, String dataset, String dataLocation, Log log) {
        this.bigQuery = bigQuery;
        this.projectId = projectId;
        this.dataset = dataset;
        this.dataLocation = dataLocation;
        this.log = log;
    }

    public static BigQueryServiceImpl.Builder builder() {
        return new BigQueryServiceImpl.Builder();
    }

    @Override
    public void createDataSet(String dataset) {
        DatasetInfo.Builder builder = DatasetInfo.newBuilder(dataset);
        builder.setLocation(dataLocation);
        Dataset newDataset = bigQuery.create(builder.build());
        log.info("Dataset created: " + newDataset.getDatasetId().getDataset());
    }

    @Override
    public void createNativeTables(String dataset, String[] locations) {
        List<Resource> resources = loadResources(locations);
        processNativeSchemas(dataset, resources);
    }

    @Override
    public void createExternalTables(String dataset, String sourceUri, String formatOptions, String[] locations) {
        List<Resource> resources = loadResources(locations);
        processExternalSchemas(dataset, sourceUri, formatOptions, resources);
    }

    @Override
    public void createViews(String dataset, String[] locations) {
        List<Resource> resources = loadResources(locations);
        processViews(dataset, resources);
    }

    @Override
    public void deleteTables(String dataset) {
        Dataset ds = bigQuery.getDataset(dataset);
        if (ds != null) {
            log.info("Deleting tables from " + dataset);
            Page<Table> tablePage;
            do {
                tablePage = ds.list();
                tablePage.getValues().forEach(table -> {
                    log.info("Deleting table " + table.getTableId().getTable());
                    table.delete();
                });
            } while (tablePage.hasNextPage());
        }
    }

    @Override
    public void deleteDataset(String dataset, boolean forceDelete) {

        List<DatasetDeleteOption> deleteOptions = new ArrayList<>();
        if (forceDelete) {
            deleteOptions.add(DatasetDeleteOption.deleteContents());
        }
        log.info("Deleting dataset " + dataset + (forceDelete ? " (forced)" : ""));
        bigQuery.delete(dataset, deleteOptions.toArray(new DatasetDeleteOption[0]));
    }

    private void processNativeSchemas(String dataset, List<Resource> resources) {
        resources.forEach(resource -> {
            TableDefinition tableDefinition = loadStandardTableDefinition(resource);
            TableInfo tableInfo = createTableInfo(resource.getFilename(), dataset, tableDefinition);
            bigQuery.create(tableInfo);

            log.info("Table " + tableInfo.getTableId().getTable() + " created");
        });
    }

    private void processExternalSchemas(String dataset, String sourceUri, String formatOptions, List<Resource> resources) {
        resources.forEach(resource -> {
            TableDefinition tableDefinition = loadExternalTableDefinition(sourceUri, formatOptions, resource);
            TableInfo tableInfo = createTableInfo(resource.getFilename(), dataset, tableDefinition);
            bigQuery.create(tableInfo);

            log.info("Table " + tableInfo.getTableId().getTable() + " created");
        });
    }

    private void processViews(String dataset, List<Resource> resources) {
        resources.forEach(resource -> {
            TableDefinition viewDefinition = loadViewDefinition(resource, projectId, dataset);
            TableInfo tableInfo = createTableInfo(resource.getFilename(), dataset, viewDefinition);
            bigQuery.create(tableInfo);

            log.info("View " + tableInfo.getTableId().getTable() + " created");
        });
    }

    private TableInfo createTableInfo(String filename, String dataset, TableDefinition tableDefinition) {
        String tableName = FilenameUtils.removeExtension(filename);
        TableId tableId = TableId.of(dataset, tableName);
        return TableInfo.newBuilder(tableId, tableDefinition).build();
    }

    private static List<Resource> loadResources(String[] locations) {
        ClassLoader classLoader = currentThread().getContextClassLoader();
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(classLoader);

        SchemaLocations loc = new SchemaLocations(locations);
        List<Resource> loadedResources = new ArrayList<>();
        loc.getSchemaLocation().forEach(location -> {
            try {
                Resource[] resources = resolver.getResources(location.getDescriptor() + "/*");
                loadedResources.addAll(asList(resources));
            } catch (IOException e) {
                throw new ConfigurationException(e.getMessage(), e);
            }

        });

        return loadedResources;
    }

    public static class Builder {

        private String projectId;
        private String credentialsFile;
        private String dataset;
        private String dataLocation;
        private Log log;

        private Builder() {
        }

        public Builder projectId(String projectId) {
            this.projectId = checkNotNull(projectId);
            return this;
        }

        public Builder credentialsFile(String credentialsFile) {
            this.credentialsFile = checkNotNull(credentialsFile);
            return this;
        }

        public Builder logger(Log log) {
            this.log = checkNotNull(log);
            return this;
        }

        public Builder dataset(String dataset) {
            this.dataset = checkNotNull(dataset);
            return this;
        }

        public Builder dataLocation(String dataLocation) {
            this.dataLocation = dataLocation;
            return this;
        }

        public BigQueryServiceImpl build() {
            BigQuery bigQuery = bigQuery();
            return new BigQueryServiceImpl(bigQuery, projectId, dataset, dataLocation, log);
        }

        private BigQuery bigQuery() {
            return BigQueryOptions.newBuilder()
                    .setCredentials(loadCredentials())
                    .setProjectId(projectId)
                    .build()
                    .getService();
        }

        private GoogleCredentials loadCredentials() {
            try {
                if (credentialsFile != null) {
                    return fromStream(loadResource(credentialsFile).getInputStream());
                }
                return getApplicationDefault();
            } catch (IOException e) {
                throw new ConfigurationException("Unable to load credentials file " + credentialsFile, e);
            }
        }

        private Resource loadResource(String location) {
            ClassLoader classLoader = currentThread().getContextClassLoader();
            ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(classLoader);
            return resolver.getResource(location);
        }
    }
}
