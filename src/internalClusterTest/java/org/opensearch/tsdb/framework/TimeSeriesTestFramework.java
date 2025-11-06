/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.framework.models.IndexConfig;
import org.opensearch.tsdb.framework.models.InputDataConfig;
import org.opensearch.tsdb.framework.models.TestCase;
import org.opensearch.tsdb.framework.models.TestSetup;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;
import org.opensearch.tsdb.utils.TSDBTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base framework for time series testing
 * Extends OpenSearch's integration test infrastructure
 *
 * <p>This framework uses TSDBDocument format for ingestion to minimize duplication with TSDBEngine.
 * The TSDBDocument format is:
 * <pre>
 * {
 *   "labels": "name http_requests method GET status 200",  // space-separated key-value pairs
 *   "timestamp": 1234567890,                               // epoch millis
 *   "value": 100.5                                          // double value
 * }
 * </pre>
 *
 * <p>This format directly aligns with how TSDBEngine parses and indexes data, ensuring consistency
 * between test data ingestion and production data flow.
 *
 * <p>Index mapping is obtained directly from {@link Constants.Mapping#DEFAULT_INDEX_MAPPING} to ensure
 * it matches the actual TSDB engine mapping.
 */
@SuppressWarnings("unchecked")
public abstract class TimeSeriesTestFramework extends OpenSearchIntegTestCase {

    // Default index settings as YAML constant
    // Note: Mapping is obtained directly from Constants.Mapping.DEFAULT_INDEX_MAPPING
    private static final String DEFAULT_INDEX_SETTINGS_YAML = """
        index.refresh_interval: "1s"
        index.tsdb_engine.enabled: true
        index.queries.cache.enabled: false
        index.requests.cache.enable: false
        """;

    protected TestSetup testSetup;
    protected TestCase testCase;
    protected SearchQueryExecutor queryExecutor;
    protected List<IndexConfig> indexConfigs;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        initializeComponents();
        clearIndexIfExists();
    }

    protected void initializeComponents() {
        // Create the index configs by merging test config with framework defaults
        try {
            // Parse the default settings YAML
            ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
            Map<String, Object> defaultSettings = yamlMapper.readValue(DEFAULT_INDEX_SETTINGS_YAML, Map.class);

            // Get the actual TSDB mapping from Constants (same as engine uses)
            // This ensures test mapping matches production mapping
            Map<String, Object> defaultMapping = parseMappingFromConstants();

            // Initialize index configs list
            indexConfigs = new ArrayList<>();

            // Validate that test setup and index configs are present
            if (testSetup == null) {
                throw new IllegalStateException("Test setup is required but was null");
            }

            if (testSetup.indexConfigs() == null || testSetup.indexConfigs().isEmpty()) {
                throw new IllegalStateException("Test setup must specify at least one index configuration in index_configs");
            }

            // Validate and create index configs
            for (IndexConfig testIndexConfig : testSetup.indexConfigs()) {
                if (testIndexConfig.name() == null || testIndexConfig.name().trim().isEmpty()) {
                    throw new IllegalArgumentException("Index configuration must specify a non-empty index name");
                }

                String indexName = testIndexConfig.name();
                int shards = testIndexConfig.shards();
                int replicas = testIndexConfig.replicas();
                indexConfigs.add(new IndexConfig(indexName, shards, replicas, defaultSettings, defaultMapping));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create index config", e);
        }

        // Initialize query executor after successful config creation
        queryExecutor = new SearchQueryExecutor(client());
    }

    /**
     * Parse the TSDB engine's default mapping from Constants.
     * This ensures we use the exact same mapping as the engine.
     */
    private Map<String, Object> parseMappingFromConstants() throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        String mappingJson = Constants.Mapping.DEFAULT_INDEX_MAPPING.trim();
        return jsonMapper.readValue(mappingJson, Map.class);
    }

    protected void runBasicTest() throws Exception {
        ingestTestData();
        executeAndValidateQueries();
    }

    protected void ingestTestData() throws Exception {

        // Create all indices
        for (IndexConfig indexConfig : indexConfigs) {
            createTimeSeriesIndex(indexConfig);
        }

        // Ingest data into respective indices
        if (testCase.inputDataList() != null && !testCase.inputDataList().isEmpty()) {
            for (InputDataConfig inputDataConfig : testCase.inputDataList()) {
                List<TimeSeriesSample> samples = TimeSeriesSampleGenerator.generateSamples(inputDataConfig);
                ingestSamples(samples, inputDataConfig.indexName());
            }
        }

        // Ensure all indices are green and refreshed
        for (IndexConfig indexConfig : indexConfigs) {
            ensureGreen(indexConfig.name());
            refresh(indexConfig.name());
        }
    }

    protected void executeAndValidateQueries() throws Exception {
        queryExecutor.executeAndValidateQueries(testCase);
    }

    protected void clearIndexIfExists() throws Exception {
        for (IndexConfig indexConfig : indexConfigs) {
            String indexName = indexConfig.name();
            if (client().admin().indices().prepareExists(indexName).get().isExists()) {
                client().admin().indices().prepareDelete(indexName).get();
                // Wait for the index to be fully deleted
                assertBusy(
                    () -> { assertFalse("Index should be deleted", client().admin().indices().prepareExists(indexName).get().isExists()); }
                );
            }
        }
    }

    protected void createTimeSeriesIndex(IndexConfig indexConfig) throws Exception {
        // Merge default settings with index configuration
        Map<String, Object> allSettings = new HashMap<>(indexConfig.settings());
        allSettings.put("index.number_of_shards", indexConfig.shards());
        allSettings.put("index.number_of_replicas", indexConfig.replicas());

        Settings settings = Settings.builder().loadFromMap(allSettings).build();

        Map<String, Object> mappingConfig = indexConfig.mapping();

        client().admin().indices().prepareCreate(indexConfig.name()).setSettings(settings).setMapping(mappingConfig).get();
    }

    /**
     * Ingest time series samples using TSDBDocument format.
     * This method minimizes duplication with TSDBEngine by using the same document format
     * that TSDBEngine expects and parses.
     *
     * <p>The TSDBDocument format used here is:
     * <pre>
     * {
     *   "labels": "name http_requests method GET status 200",  // space-separated key-value pairs
     *   "timestamp": 1234567890,                               // epoch millis
     *   "value": 100.5                                          // double value
     * }
     * </pre>
     *
     * <p>This directly maps to how TSDBEngine's TSDBDocument.fromParsedDocument() expects data,
     * ensuring that test ingestion follows the same code path as production ingestion.
     *
     * @param samples The list of samples to ingest
     * @param indexName The name of the index to ingest data into
     * @throws Exception if ingestion fails
     */
    protected void ingestSamples(List<TimeSeriesSample> samples, String indexName) throws Exception {
        // Use bulk request for better performance, but still one document per sample
        BulkRequest bulkRequest = new BulkRequest();

        // Send one document per sample to match production data shape
        for (TimeSeriesSample sample : samples) {
            Map<String, String> labels = sample.labels();
            ByteLabels byteLabels = ByteLabels.fromMap(labels);

            // Create TSDBDocument format JSON using the utility method
            // This ensures consistency with TSDBEngine's document parsing
            String documentJson = TSDBTestUtils.createTSDBDocumentJson(sample);
            String seriesId = byteLabels.toString();

            IndexRequest request = new IndexRequest(indexName).source(documentJson, XContentType.JSON).routing(seriesId);
            bulkRequest.add(request);
        }

        // Execute bulk request
        if (bulkRequest.numberOfActions() > 0) {
            BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
            if (bulkResponse.hasFailures()) {
                throw new RuntimeException("Bulk ingestion failed: " + bulkResponse.buildFailureMessage());
            }
        }

        client().admin().indices().prepareFlush(indexName).get();
        client().admin().indices().prepareRefresh(indexName).get();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settingsBuilder = Settings.builder().put(super.nodeSettings(nodeOrdinal));

        if (testSetup != null && testSetup.nodeSettings() != null) {
            for (Map.Entry<String, Object> entry : testSetup.nodeSettings().entrySet()) {
                settingsBuilder.put(entry.getKey(), entry.getValue().toString());
            }
        }

        return settingsBuilder.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return super.nodePlugins();
    }

    @Override
    protected boolean addMockInternalEngine() {
        // Disable MockEngineFactoryPlugin to avoid conflicts with TSDBEngine
        return false;
    }

    protected void loadTestConfigurationFromFile(String yamlFilePath) throws Exception {
        testSetup = YamlLoader.loadTestSetup(yamlFilePath);
        testCase = YamlLoader.loadTestCase(yamlFilePath);
    }

}
