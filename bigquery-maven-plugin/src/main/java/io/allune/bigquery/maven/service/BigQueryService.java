package io.allune.bigquery.maven.service;

import java.util.List;

public interface BigQueryService {

    void createDataset(String dataLocation);

    void createNativeTables(List<String> locations);

    void createExternalTables(String sourceUri, String formatOptions, List<String> locations);

    void createViews(List<String> locations);

    void deleteTables();

    void deleteDataset(boolean forceDelete);
}
