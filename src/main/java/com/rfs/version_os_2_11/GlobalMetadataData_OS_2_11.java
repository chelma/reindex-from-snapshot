package com.rfs.version_os_2_11;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class GlobalMetadataData_OS_2_11 implements com.rfs.common.GlobalMetadata.Data {
    private final ObjectNode root;

    public GlobalMetadataData_OS_2_11(ObjectNode root) {
        this.root = root;
    }

    public ObjectNode toObjectNode() throws Exception {
        return root;
    }

    public ObjectNode getTemplates() throws Exception {
        return (ObjectNode) root.get("templates");
    }

    public ObjectNode getIndexTemplates() throws Exception {
        return (ObjectNode) root.get("index_template");
    }

    public ObjectNode getComponentTemplates() throws Exception {
        return (ObjectNode) root.get("component_template");
    }
}
