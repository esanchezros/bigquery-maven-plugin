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