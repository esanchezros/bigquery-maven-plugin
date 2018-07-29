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

package io.allune.bigquery.maven.service;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetDeleteOption;
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
    private final Log log;

    private BigQueryServiceImpl(BigQuery bigQuery, String projectId, String dataset, Log log) {
        this.bigQuery = bigQuery;
        this.projectId = projectId;
        this.dataset = dataset;
        this.log = log;
    }

    public static BigQueryServiceImpl.Builder builder() {
        return new BigQueryServiceImpl.Builder();
    }

    @Override
    public void createDataset(String dataLocation) {
        DatasetInfo.Builder builder = DatasetInfo.newBuilder(dataset);
        builder.setLocation(dataLocation);
        bigQuery.create(builder.build());

        log.info("Dataset created: " + dataset);
    }

    @Override
    public void createNativeTables(List<String> schemaLocations) {
        List<Resource> resources = loadResources(schemaLocations);
        resources.forEach(resource -> {
            TableDefinition tableDefinition = loadStandardTableDefinition(resource);
            TableInfo tableInfo = createTableInfo(resource.getFilename(), dataset, tableDefinition);
            bigQuery.create(tableInfo);

            log.info("Table " + tableInfo.getTableId().getTable() + " created");
        });
    }

    @Override
    public void createExternalTables(String sourceUri, String formatOptions, List<String> schemaLocations) {
        List<Resource> resources = loadResources(schemaLocations);
        resources.forEach(resource -> {
            TableDefinition tableDefinition = loadExternalTableDefinition(sourceUri, formatOptions, resource);
            TableInfo tableInfo = createTableInfo(resource.getFilename(), dataset, tableDefinition);
            bigQuery.create(tableInfo);

            log.info("Table " + tableInfo.getTableId().getTable() + " created");
        });
    }

    @Override
    public void createViews(List<String> schemaLocations) {
        List<Resource> resources = loadResources(schemaLocations);
        resources.forEach(resource -> {
            TableDefinition viewDefinition = loadViewDefinition(resource, projectId, dataset);
            TableInfo tableInfo = createTableInfo(resource.getFilename(), dataset, viewDefinition);
            bigQuery.create(tableInfo);

            log.info("View " + tableInfo.getTableId().getTable() + " created");
        });
    }

    @Override
    public void deleteTables() {
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
    public void deleteDataset(boolean forceDelete) {
        List<DatasetDeleteOption> deleteOptions = new ArrayList<>();
        if (forceDelete) {
            deleteOptions.add(DatasetDeleteOption.deleteContents());
        }

        log.info("Deleting dataset " + dataset + (forceDelete ? " (forced)" : ""));
        bigQuery.delete(dataset, deleteOptions.toArray(new DatasetDeleteOption[0]));
    }

    private static List<Resource> loadResources(List<String> locations) {
        ClassLoader classLoader = currentThread().getContextClassLoader();
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(classLoader);

        SchemaLocations loc = new SchemaLocations(locations);
        List<Resource> loadedResources = new ArrayList<>();
        loc.getSchemaLocations().forEach(location -> {
            try {
                Resource resource = resolver.getResource(location.getDescriptor());
                if (resource != null && resource.isReadable()) {
                    loadedResources.add(resource);
                } else {
                    Resource[] resources = resolver.getResources(location.getDescriptor() + "/*");
                    loadedResources.addAll(asList(resources));
                }
            } catch (IOException e) {
                throw new ConfigurationException(e.getMessage(), e);
            }
        });

        return loadedResources;
    }

    private TableInfo createTableInfo(String filename, String dataset, TableDefinition tableDefinition) {
        String tableName = FilenameUtils.removeExtension(filename);
        TableId tableId = TableId.of(dataset, tableName);
        return TableInfo.newBuilder(tableId, tableDefinition).build();
    }

    public static class Builder {

        private BigQuery bigQuery;
        private String projectId;
        private String credentialsFile;
        private String dataset;
        private Log log;

        private Builder() {
            // no op
        }

        public Builder bigQuery(BigQuery bigQuery) {
            this.bigQuery = bigQuery;
            return this;
        }

        public Builder projectId(String projectId) {
            this.projectId = projectId;
            return this;
        }

        public Builder credentialsFile(String credentialsFile) {
            this.credentialsFile = credentialsFile;
            return this;
        }

        public Builder dataset(String dataset) {
            this.dataset = dataset;
            return this;
        }

        public Builder logger(Log log) {
            this.log = log;
            return this;
        }

        public BigQueryServiceImpl build() {
            checkNotNull(bigQuery, "bigQuery is null");
            checkNotNull(projectId, "projectId is null");
            checkNotNull(dataset, "dataset is null");
            checkNotNull(credentialsFile, "credentialsFile is null");
            checkNotNull(log, "log is null");

            return new BigQueryServiceImpl(bigQuery, projectId, dataset, log);
        }
    }
}
