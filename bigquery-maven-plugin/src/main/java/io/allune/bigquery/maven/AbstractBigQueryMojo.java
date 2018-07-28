package io.allune.bigquery.maven;

import io.allune.bigquery.maven.service.BigQueryServiceImpl;
import org.apache.maven.artifact.DependencyResolutionRequiredException;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    protected String datasetName;

    /**
     * The location of the data. EU by default
     */
    @Parameter(alias = "dataLocation", property = "bigquery.dataLocation", defaultValue = "EU")
    protected String dataLocation;

    /**
     * The credentials file required to authenticate with BigQuery
     */
    @Parameter(alias = "credentialsFile", property = "bigquery.credentialsFile", required = true)
    private String credentialsFile;

    @Parameter(defaultValue = "${project}", required = true, readonly = true)
    protected MavenProject mavenProject;

    protected Log log;

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
                .projectId(projectId)
                .credentialsFile(credentialsFile)
                .dataset(datasetName)
                .dataLocation(dataLocation)
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

    /**
     * Executes this mojo.
     */
    abstract void doExecute(BigQueryServiceImpl bigQueryService) throws MojoExecutionException;
}
