package com.rfs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class GlobalMetadata {
    private final ObjectNode root;

    public static GlobalMetadata fromJsonNode(JsonNode root) throws Exception {
        ObjectNode metadataRoot = (ObjectNode) root.get("meta-data");
        return new GlobalMetadata(metadataRoot);
    }

    public GlobalMetadata(ObjectNode root) {
        this.root = root;
    }

    public ObjectNode getTemplates() throws Exception {
        return (ObjectNode) root.get("templates");
    }
    
}
