# BigQuery Maven plugin

This Maven plugin provides goals to create datasets, tables and views in Google BigQuery.

# How to use

In your application, add the following plugin to your pom.xml:

```XML
<plugin>
    <groupId>com.allune</groupId>
    <artifactId>bigquery-maven-plugin</artifactId>
    <version>1.0.0</version>
</plugin>
```

# Supported goals

Goal | Description
--- | ---
bigquery:create|Creates the dataset, tables and views defined in the plugin configuration.
bigquery:create-dataset|Creates the dataset defined in the plugin configuration.
bigquery:clean|Removes the dataset, tables and views defined in the plugin configuration.
bigquery:help|Displays help information on the plugin. Use `mvn bigquery:help -Ddetail=true -Dgoal=[goal]` for detailed goal documentation.


# Example plugin configuration

```XML
<plugin>
    <groupId>com.allune</groupId>
    <artifactId>bigquery-maven-plugin</artifactId>
    <version>1.0-SNAPSHOT</version>
    <configuration>
        <projectId>your_project_id</projectId>
        <credentialsFile>/credentials.json</credentialsFile>
        <datasetName>your_dataset</datasetName>
        <dataLocation>EU</dataLocation>
    </configuration>
    <executions>
        <execution>
            <id>create</id>
            <goals>
                <goal>create</goal>
            </goals>
            <phase>pre-integration-test</phase>
            <configuration>
                <skip>${skipTests}</skip>
                <createDataset>true</createDataset>
                <sourceUri>gs://folder/data.json</sourceUri>
                <formatOptions>CSV</formatOptions>
                <nativeSchemaLocations>file://${project.basedir}/src/main/resources/bigquery/schemas/dir1</nativeSchemaLocations>
                <externalSchemaLocations>classpath:/bigquery/schemas/dir2</externalSchemaLocations>
                <viewLocations>file://${project.basedir}/src/main/resources/bigquery/views</viewLocations>
            </configuration>
        </execution>
        <execution>
            <id>clean</id>
            <goals>
                <goal>clean</goal>
            </goals>
            <phase>post-integration-test</phase>
            <configuration>
                <skip>${skipTests}</skip>
                <deleteDataset>true</deleteDataset>
                <forceDeleteDataset>true</forceDeleteDataset>
            </configuration>
        </execution>
    </executions>
</plugin>
```