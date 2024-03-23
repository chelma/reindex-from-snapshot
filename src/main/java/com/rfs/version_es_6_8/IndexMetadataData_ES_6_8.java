package com.rfs.version_es_6_8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class IndexMetadataData_ES_6_8 implements com.rfs.common.IndexMetadata.Data {
    private ObjectNode root;
    private ObjectNode mappings;
    private ObjectNode settings;
    private String indexId;
    private String indexName;

    public IndexMetadataData_ES_6_8(ObjectNode root, String indexId, String indexName) {
        this.root = root;
        this.mappings = null;
        this.settings = null;
        this.indexId = indexId;
        this.indexName = indexName;
    }

    public ObjectNode getAliases() {
        return (ObjectNode) root.get("aliases");
    }

    public String getId() {
        return indexId;
    }

    public ObjectNode getMappings() {
        if (mappings != null) {
            return mappings;
        }

        // Extract the mappings from their single-member list, will start like:
        // [{"_doc":{"properties":{"address":{"type":"text"}}}}]
        ArrayNode mappingsArray = (ArrayNode) root.get("mappings");
        ObjectNode mappingsNode = (ObjectNode) mappingsArray.get(0).get("_doc");
        mappings = mappingsNode;

        return mappings;
    }

    public String getName() {
        return indexName;
    }

    public int getNumberOfShards() {
        return this.getSettings().get("index").get("number_of_shards").asInt();
    }   

    public ObjectNode getSettings() {
        if (settings != null) {
            return settings;
        }

        // Turn dotted index settings into a tree, will start like:
        // {"index.number_of_replicas":"1","index.number_of_shards":"5","index.version.created":"6082499"}
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode flatSettings = (ObjectNode) root.get("settings");
        ObjectNode treeSettings = mapper.createObjectNode();

        flatSettings.fields().forEachRemaining(entry -> {
            String[] parts = entry.getKey().split("\\.");
            ObjectNode current = treeSettings;

            for (int i = 0; i < parts.length - 1; i++) {
                if (!current.has(parts[i])) {
                    current.set(parts[i], mapper.createObjectNode());
                }
                current = (ObjectNode) current.get(parts[i]);
            }

            current.set(parts[parts.length - 1], entry.getValue());
        });
        settings = treeSettings;

        return settings;
    }
}
