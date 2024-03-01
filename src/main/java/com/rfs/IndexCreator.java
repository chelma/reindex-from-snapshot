package com.rfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class IndexCreator {
    public static void createIndex(String targetName, IndexMetadataProvider indexMetadataProvider, ConnectionDetails connectionDetails) throws Exception {
        // Assemble the request body
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        root.set("aliases", indexMetadataProvider.getAliasesJson());
        root.set("mappings", indexMetadataProvider.getMappingsJson());
        root.set("settings", indexMetadataProvider.getSettingsJson());
        String body = root.toString();

        // Send the request
        RestClient client = new RestClient(connectionDetails);
        client.put(targetName, body);
    }
}
