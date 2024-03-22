package com.rfs.source_es_7_10;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.rfs.common.ShardMetadata;

public class ShardMetadataFactory_ES_7_10 implements ShardMetadata.Factory {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public ShardMetadata.Data fromJsonNode(JsonNode root, String indexId, String indexName, int shardId) throws Exception {
        ObjectNode objectNodeRoot = (ObjectNode) root;
        ShardMetadataData_ES_7_10.DataRaw shardMetadataRaw = objectMapper.treeToValue(objectNodeRoot, ShardMetadataData_ES_7_10.DataRaw.class);
        return new ShardMetadataData_ES_7_10(
                shardMetadataRaw.name,
                indexName,
                indexId,
                shardId,
                shardMetadataRaw.indexVersion,
                shardMetadataRaw.startTime,
                shardMetadataRaw.time,
                shardMetadataRaw.numberOfFiles,
                shardMetadataRaw.totalSize,
                shardMetadataRaw.files
        );
    }

    public SmileFactory getSmileFactory() {
        return ElasticsearchConstants_ES_7_10.SMILE_FACTORY;
    }
}
