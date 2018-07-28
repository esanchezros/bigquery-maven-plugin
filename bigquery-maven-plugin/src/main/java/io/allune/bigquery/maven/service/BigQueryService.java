package io.allune.bigquery.maven.service;

public interface BigQueryService {

    void createDataSet(String dataset);

    void createNativeTables(String dataset, String[] locations);

    void createExternalTables(String dataset, String sourceUri, String formatOptions, String[] locations);

    void createViews(String dataset, String[] locations);

    void deleteTables(String dataset);

    void deleteDataset(String dataset, boolean forceDelete);
}
