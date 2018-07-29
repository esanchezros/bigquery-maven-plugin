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
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * Maven goal to delete the BigQuery tables and dataset
 */
@Mojo(name = "clean")
public class CleanMojo extends AbstractBigQueryMojo {

    /**
     * Whether to delete the dataset as part of the clean up
     */
    @Parameter(alias = "deleteDataset", property = "bigquery.deleteDataset", defaultValue = "false")
    private boolean deleteDataset;

    @Parameter(alias = "deleteTables", property = "bigquery.deleteTables", defaultValue = "false")
    private boolean deleteTables;

    @Parameter(alias = "forceDeleteDataset", property = "bigquery.forceDeleteDataset", defaultValue = "false")
    private boolean forceDeleteDataset;

    public void setDeleteDataset(boolean deleteDataset) {
        this.deleteDataset = deleteDataset;
    }

    public void setDeleteTables(boolean deleteTables) {
        this.deleteTables = deleteTables;
    }

    public void setForceDeleteDataset(boolean forceDeleteDataset) {
        this.forceDeleteDataset = forceDeleteDataset;
    }

    @Override
    protected void doExecute(BigQueryServiceImpl bigQueryService) throws MojoExecutionException {
        try {

            if (forceDeleteDataset) {
                bigQueryService.deleteDataset(true);
            } else {

                if (deleteTables) {
                    bigQueryService.deleteTables();
                }
                if (deleteDataset) {
                    bigQueryService.deleteDataset(false);
                }
            }
        } catch (BigQueryException e) {
            throw new MojoExecutionException(e.getMessage(), e);
        }
    }
}