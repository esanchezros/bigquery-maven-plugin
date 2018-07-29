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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import io.allune.bigquery.maven.service.BigQueryServiceImpl;
import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.auth.oauth2.GoogleCredentials.getApplicationDefault;
import static com.google.auth.oauth2.ServiceAccountCredentials.fromStream;
import static java.lang.Thread.currentThread;

/**
 * Common base class for all mojos with all common attributes.<br>
 */
@SuppressWarnings("unused")
public abstract class AbstractBigQueryMojo extends AbstractMojo {

    /**
     * Whether the execution of this mojo should be skipped
     */
    @Parameter(alias = "skip", property = "bigquery.skip", defaultValue = "false")
    private boolean skip;

    /**
     * The BigQuery project identifier
     */
    @Parameter(alias = "projectId", property = "bigquery.projectId", required = true)
    private String projectId;

    /**
     * The dataset to use for creating the BigQuery tables
     */
    @Parameter(alias = "datasetName", property = "bigquery.datasetName", required = true)
    private String datasetName;

    /**
     * The location of the data. EU by default
     */
    @Parameter(alias = "dataLocation", property = "bigquery.dataLocation", defaultValue = "EU")
    private String dataLocation;

    /**
     * The credentials file required to authenticate with BigQuery
     */
    @Parameter(alias = "credentialsFile", property = "bigquery.credentialsFile", required = true)
    private String credentialsFile;

    @Parameter(defaultValue = "${project}", required = true, readonly = true)
    private MavenProject mavenProject;

    private Log log;

    public boolean isSkip() {
        return skip;
    }

    public void setSkip(boolean skip) {
        this.skip = skip;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public String getDataLocation() {
        return dataLocation;
    }

    public void setDataLocation(String dataLocation) {
        this.dataLocation = dataLocation;
    }

    public String getCredentialsFile() {
        return credentialsFile;
    }

    public void setCredentialsFile(String credentialsFile) {
        this.credentialsFile = credentialsFile;
    }

    public void execute() throws MojoExecutionException {
        log = getLog();

        if (skip) {
            log.info("Skipping BigQuery execution");
            return;
        }

        // Add project classpath elements into classloader
        enhanceClassloader();

        // Create BigQuery service with the provided credentials
        BigQueryServiceImpl bigQueryService = BigQueryServiceImpl.builder()
                .bigQuery(bigQuery())
                .projectId(projectId)
                .credentialsFile(credentialsFile)
                .dataset(datasetName)
                .logger(log)
                .build();

        // Execute this mojo
        doExecute(bigQueryService);
    }

    private void enhanceClassloader() {
        try {
            Set<URL> urls = new HashSet<>();
            List<Stream<String>> streams = new ArrayList<>();
            streams.add(mavenProject.getRuntimeClasspathElements().stream());
            streams.add(mavenProject.getCompileClasspathElements().stream());
            streams.add(mavenProject.getTestClasspathElements().stream());

            for (String element : streams.stream().flatMap(s -> s).collect(Collectors.toList())) {
                urls.add(new File(element).toURI().toURL());
            }

            ClassLoader contextClassLoader = URLClassLoader.newInstance(
                    urls.toArray(new URL[0]),
                    Thread.currentThread().getContextClassLoader());

            Thread.currentThread().setContextClassLoader(contextClassLoader);

        } catch (DependencyResolutionRequiredException | MalformedURLException e) {
            throw new ConfigurationException(e.getMessage(), e);
        }
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

    /**
     * Executes this mojo.
     */
    abstract void doExecute(BigQueryServiceImpl bigQueryService) throws MojoExecutionException;
}
