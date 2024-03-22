package com.rfs.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rfs.source_es_6_8.IndexMetadata;
import com.rfs.source_es_6_8.Transformer;

public class IndexCreator {
    public static void create(String targetName, IndexMetadata indexMetadata, ConnectionDetails connectionDetails, Transformer transformer) throws Exception {
        // Remove some settings which will cause errors if you try to pass them to the API
        ObjectNode settings = indexMetadata.getSettings();

        if (settings.has("index") && settings.get("index").isObject()) {
            ObjectNode indexNode = (ObjectNode) settings.get("index");

            String[] problemFields = {"creation_date", "provided_name", "uuid", "version"};
            for (String field : problemFields) {
                indexNode.remove(field);
            }
        }

        // Assemble the request body
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        root.set("aliases", indexMetadata.getAliases());
        root.set("mappings", indexMetadata.getMappings());
        root.set("settings", settings);

        // Transform the body as necessary
        ObjectNode transformedRoot = transformer.transformIndexBody(root);

        // Send the request
        String body = transformedRoot.toString();
        RestClient client = new RestClient(connectionDetails);
        client.put(targetName, body);
    }
}
