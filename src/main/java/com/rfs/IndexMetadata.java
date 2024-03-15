package com.rfs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class IndexMetadata {
    private final ObjectNode root;
    private final String indexId;
    private final String indexName;

    public static IndexMetadata fromJsonNode(JsonNode root, String indexId, String indexName) throws Exception {
        ObjectNode objectNodeRoot = (ObjectNode) root.get(indexName);

        // Turn dotted index settings into a tree, will start like:
        // {"index.number_of_replicas":"1","index.number_of_shards":"5","index.version.created":"6082499"}
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode flatSettings = (ObjectNode) objectNodeRoot.get("settings");
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

        objectNodeRoot.set("settings", treeSettings);

        // Extract the mappings from their single-member list, will start like:
        // [{"_doc":{"properties":{"address":{"type":"text"}}}}]
        ArrayNode mappings = (ArrayNode) objectNodeRoot.get("mappings");
        ObjectNode mappingsRoot = (ObjectNode) mappings.get(0).get("_doc");
        objectNodeRoot.set("mappings", mappingsRoot);

        return new IndexMetadata(objectNodeRoot, indexId, indexName);
    }

    public IndexMetadata(ObjectNode root, String indexId, String indexName) {
        this.root = root;
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
        return (ObjectNode) root.get("mappings");
    }

    public String getName() {
        return indexName;
    }

    public int getNumberOfShards() {
        return root.get("settings").get("index").get("number_of_shards").asInt();
    }   

    public ObjectNode getSettings() {
        return (ObjectNode) root.get("settings");
    }
}
