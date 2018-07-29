package io.allune.bigquery.maven.service;

import java.util.List;

public interface BigQueryService {

    void createDataSet(String dataset);

    void createNativeTables(String dataset, List<String> locations);

    void createExternalTables(String dataset, String sourceUri, String formatOptions, List<String> locations);

    void createViews(String dataset, List<String> locations);

    void deleteTables(String dataset);

    void deleteDataset(String dataset, boolean forceDelete);
}
