package com.rfs;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Arrays;

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;

public class IndexCreator {

    protected static String getCreateRestPath(String targetName) {
        return "/" + targetName;
    }
    protected static ObjectNode getMetadataJson(IndexMetadata indexMetadata) throws Exception {
        // Will be something like:
        // {"_doc":{"properties":{"address":{"type":"text"}}}}
        String rawMetadata = indexMetadata.mapping().source().toString();

        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootJson = mapper.readTree(rawMetadata);
        ObjectNode root = (ObjectNode) rootJson;
        
        return (ObjectNode) root.get("_doc");
    }

    protected static ObjectNode getAliasesJson(IndexMetadata indexMetadata) throws Exception {

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        for (ObjectCursor<String> aliasName : indexMetadata.getAliases().keys()) {
            AliasMetadata aliasMetadata = indexMetadata.getAliases().get(aliasName.value);
            String aliasString = aliasMetadata.toString();
            JsonNode rootJson = mapper.readTree(aliasString);
            root.set(aliasName.value, (ObjectNode) rootJson.get(aliasName.value));
        }
        
        return root;        
    }

    protected static ObjectNode getSettingsJson(IndexMetadata indexMetadata) {
        // Will be something like:
        // index.creation_date=1709172482837,index.number_of_replicas=1,index.number_of_shards=1,index.provided_name=index_2,
        // index.routing.allocation.include._tier_preference=data_content,index.uuid=a1YVzezrRpCw_XiAW7yyLg,index.version.created=7100299,
        String rawSettings = indexMetadata.getSettings().toDelimitedString(',');
        String[] pairs = rawSettings.split(",");

        // Remove some settings will cause errors if you try to pass them to the API
        String[] toRemove = {"index.creation_date", "index.provided_name", "index.uuid", "index.version.created"};
        String[] filteredPairs = Arrays.stream(pairs)
            .filter(part -> Arrays.stream(toRemove).noneMatch(part::startsWith))
            .toArray(String[]::new);;

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();

        for (String pair : filteredPairs) {
            String[] keyValue = pair.split("=");
            String key = keyValue[0];
            String value = keyValue[1];
            
            String[] parts = key.split("\\.");
            ObjectNode current = root;
            
            for (int i = 0; i < parts.length - 1; i++) {
                String part = parts[i];
                if (!current.has(part)) {
                    current.set(part, mapper.createObjectNode());
                }
                current = (ObjectNode) current.get(part);
            }
            
            current.put(parts[parts.length - 1], value);
        }
        
        return root;
    }

    protected static String getCreateRequestBody(IndexMetadata indexMetadata) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();

        ObjectNode settingsNode = root.putObject("settings");
        settingsNode.put("number_of_replicas", indexMetadata.getNumberOfReplicas());
        settingsNode.put("number_of_shards", indexMetadata.getNumberOfShards());


        return indexMetadata.toString();
    }

    public static void createIndex(String targetName, IndexMetadata indexMetadata, ConnectionDetails connectionDetails) throws Exception {
        // Assemble the request body
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        root.set("aliases", getAliasesJson(indexMetadata));
        root.set("mappings", getMetadataJson(indexMetadata));
        root.set("settings", getSettingsJson(indexMetadata));
        String body = root.toString();

        // Send the request
        RestClient client = new RestClient(connectionDetails);
        client.put(targetName, body);
    }
}
