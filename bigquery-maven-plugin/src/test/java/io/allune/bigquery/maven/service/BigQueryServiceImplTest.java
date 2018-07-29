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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.TableInfo;
import com.google.common.collect.ImmutableList;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.plugin.logging.SystemStreamLog;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryServiceImplTest {

    @Mock
    private BigQuery bigQuery;

    @Captor
    private ArgumentCaptor<DatasetInfo> datasetInfoCapture;

    @Captor
    private ArgumentCaptor<TableInfo> tableInfoCapture;

    @Test
    public void shouldThrowExceptionIfBigQueryNotPresent() {
        try {
            // Given When
            BigQueryServiceImpl.builder().logger(new SystemStreamLog()).dataset("").credentialsFile("").projectId("").build();
            fail("Expected NullPointerException");
        } // Then
        catch (NullPointerException ex) {
            // Expected
            assertThat(ex.getMessage()).isEqualTo("bigQuery is null");
        }
    }

    @Test
    public void shouldThrowExceptionIfProjectIdNotPresent() {
        try {
            // Given When

            BigQueryServiceImpl.builder().bigQuery(bigQuery).credentialsFile("").dataset("").build();
            fail("Expected NullPointerException");
        } // Then
        catch (NullPointerException ex) {
            // Expected
            assertThat(ex.getMessage()).isEqualTo("projectId is null");
        }
    }

    @Test
    public void shouldThrowExceptionIfCredentialsFileNotPresent() {
        try {
            // Given When
            BigQueryServiceImpl.builder().bigQuery(bigQuery).projectId("").dataset("").build();
            fail("Expected NullPointerException");
        } // Then
        catch (NullPointerException ex) {
            // Expected
            assertThat(ex.getMessage()).isEqualTo("credentialsFile is null");
        }
    }

    @Test
    public void shouldThrowExceptionIfDatasetNotPresent() {
        try {
            // Given When
            BigQueryServiceImpl.builder().bigQuery(bigQuery).credentialsFile("").projectId("").build();
            fail("Expected NullPointerException");
        } // Then
        catch (NullPointerException ex) {
            // Expected
            assertThat(ex.getMessage()).isEqualTo("dataset is null");
        }
    }

    @Test
    public void shouldThrowExceptionIfLogNotPresent() {
        try {
            // Given When
            BigQueryServiceImpl.builder().bigQuery(bigQuery).dataset("").credentialsFile("").projectId("").build();
            fail("Expected NullPointerException");
        } // Then
        catch (NullPointerException ex) {
            // Expected
            assertThat(ex.getMessage()).isEqualTo("log is null");
        }
    }

    @Test
    public void testCreateDataset() {
        String expectedDataset = "anyDataset";
        String expectedDataLocation = "anyDataLocation";
        BigQueryService service = BigQueryServiceImpl.builder()
                .bigQuery(bigQuery)
                .projectId("")
                .credentialsFile("")
                .dataset(expectedDataset)
                .logger(mock(Log.class))
                .build();

        service.createDataset(expectedDataLocation);
        verify(bigQuery).create(datasetInfoCapture.capture());
        assertThat(datasetInfoCapture.getValue().getDatasetId().getDataset()).isEqualTo(expectedDataset);
        assertThat(datasetInfoCapture.getValue().getLocation()).isEqualTo(expectedDataLocation);
    }

    @Test
    public void testCreateNativeTablesFromFile() {
        String expectedDataset = "anyDataset";
        BigQueryService service = BigQueryServiceImpl.builder()
                .bigQuery(bigQuery)
                .projectId("")
                .credentialsFile("")
                .dataset(expectedDataset)
                .logger(mock(Log.class))
                .build();

        service.createNativeTables(ImmutableList.of("classpath:/dir/test_table_1.json"));
        verify(bigQuery).create(tableInfoCapture.capture());
        assertThat(tableInfoCapture.getValue().getTableId().getTable()).isEqualTo("test_table_1");
        assertThat(tableInfoCapture.getValue().getTableId().getDataset()).isEqualTo(expectedDataset);
    }

    @Test
    public void testCreateNativeTablesFromFolder() {
        String expectedDataset = "anyDataset";
        BigQueryService service = BigQueryServiceImpl.builder()
                .bigQuery(bigQuery)
                .projectId("")
                .credentialsFile("")
                .dataset(expectedDataset)
                .logger(mock(Log.class))
                .build();

        service.createNativeTables(ImmutableList.of("classpath:/dir"));
        verify(bigQuery).create(tableInfoCapture.capture());
        assertThat(tableInfoCapture.getValue().getTableId().getTable()).isEqualTo("test_table_1");
        assertThat(tableInfoCapture.getValue().getTableId().getDataset()).isEqualTo(expectedDataset);
    }

    @Test
    public void testCreateExternalTablesFromFile() {
        String expectedDataset = "anyDataset";
        BigQueryService service = BigQueryServiceImpl.builder()
                .bigQuery(bigQuery)
                .projectId("")
                .credentialsFile("")
                .dataset(expectedDataset)
                .logger(mock(Log.class))
                .build();

        service.createExternalTables("", "", ImmutableList.of("classpath:/dir/test_table_1.json"));
        verify(bigQuery).create(tableInfoCapture.capture());
        assertThat(tableInfoCapture.getValue().getTableId().getTable()).isEqualTo("test_table_1");
        assertThat(tableInfoCapture.getValue().getTableId().getDataset()).isEqualTo(expectedDataset);
    }

    @Test
    public void testCreateExternalTablesFromFolder() {
        String expectedDataset = "anyDataset";
        BigQueryService service = BigQueryServiceImpl.builder()
                .bigQuery(bigQuery)
                .projectId("")
                .credentialsFile("")
                .dataset(expectedDataset)
                .logger(mock(Log.class))
                .build();

        service.createExternalTables("", "", ImmutableList.of("classpath:/dir"));
        verify(bigQuery).create(tableInfoCapture.capture());
        assertThat(tableInfoCapture.getValue().getTableId().getTable()).isEqualTo("test_table_1");
        assertThat(tableInfoCapture.getValue().getTableId().getDataset()).isEqualTo(expectedDataset);
    }

    @Test
    public void testCreateViewsFromFile() {
        String expectedDataset = "anyDataset";
        BigQueryService service = BigQueryServiceImpl.builder()
                .bigQuery(bigQuery)
                .projectId("")
                .credentialsFile("")
                .dataset(expectedDataset)
                .logger(mock(Log.class))
                .build();

        service.createViews(ImmutableList.of("classpath:/dir/test_table_1.json"));
        verify(bigQuery).create(tableInfoCapture.capture());
        assertThat(tableInfoCapture.getValue().getTableId().getTable()).isEqualTo("test_table_1");
        assertThat(tableInfoCapture.getValue().getTableId().getDataset()).isEqualTo(expectedDataset);
    }

    @Test
    public void testCreateViewsFromFolder() {
        String expectedDataset = "anyDataset";
        BigQueryService service = BigQueryServiceImpl.builder()
                .bigQuery(bigQuery)
                .projectId("")
                .credentialsFile("")
                .dataset(expectedDataset)
                .logger(mock(Log.class))
                .build();

        service.createViews(ImmutableList.of("classpath:/dir"));
        verify(bigQuery).create(tableInfoCapture.capture());
        assertThat(tableInfoCapture.getValue().getTableId().getTable()).isEqualTo("test_table_1");
        assertThat(tableInfoCapture.getValue().getTableId().getDataset()).isEqualTo(expectedDataset);
    }
}