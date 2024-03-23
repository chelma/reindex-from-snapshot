package com.rfs.version_es_6_8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.rfs.common.Transformer;

public class Transformer_ES_6_8_to_OS_2_11 implements Transformer {
    private static final ObjectMapper mapper = new ObjectMapper();    
    private int awarenessAttributeDimensionality;

    public Transformer_ES_6_8_to_OS_2_11(int awarenessAttributeDimensionality) {
        this.awarenessAttributeDimensionality = awarenessAttributeDimensionality;
    }

    public ObjectNode transformGlobalMetadata(ObjectNode root) {
        ObjectNode newRoot = mapper.createObjectNode();

        // Transform the original "templates", but put them into the legacy "templates" bucket on the target
        ObjectNode templatesRoot = (ObjectNode) root.get("templates").deepCopy();
        templatesRoot.fieldNames().forEachRemaining(templateName -> {
            ObjectNode template = (ObjectNode) templatesRoot.get(templateName);
            removeIntermediateMappingsLevel(template);
            removeIntermediateIndexSettingsLevel(template); // run before fixNumberOfReplicas
            fixNumberOfReplicas(template);
            templatesRoot.set(templateName, template);
        });
        newRoot.set("templates", templatesRoot);

        System.out.println("Original Object: " + root.toString());
        System.out.println("Transformed Object: " + newRoot.toString());
        return newRoot;
    }

    public ObjectNode transformIndexMetadata(ObjectNode root){        
        ObjectNode newRoot = root.deepCopy();

        removeIntermediateMappingsLevel(newRoot);
        removeIntermediateIndexSettingsLevel(newRoot); // run before fixNumberOfReplicas
        fixNumberOfReplicas(newRoot);

        System.out.println("Original Object: " + root.toString());
        System.out.println("Transformed Object: " + newRoot.toString());
        return newRoot;
    }

    /**
     * If the object has mappings, then we need to ensure they aren't burried underneath an intermediate key.
     * As an example, if we had mappings._doc.properties, we need to make it mappings.properties.
     */ 
    private void removeIntermediateMappingsLevel(ObjectNode root) {        
        if (root.has("mappings")) {
            // Extract the mappings from their single-member list, will start like:
            // [{"_doc":{"properties":{"address":{"type":"text"}}}}]
            try {
                ArrayNode mappingsList = (ArrayNode) root.get("mappings");
                ObjectNode actualMappingsRoot = (ObjectNode) mappingsList.get(0);
                if (actualMappingsRoot.has("_doc")) {
                    root.set("mappings", (ObjectNode) actualMappingsRoot.get("_doc"));
                } else if (actualMappingsRoot.has("doc")) {
                    root.set("mappings", (ObjectNode) actualMappingsRoot.get("doc"));
                }
            } catch (ClassCastException e) {
                // mappings isn't an array
                return;
            }            
        }
    }
    /**
     * If the object has settings, then we need to ensure they aren't burried underneath an intermediate key.
     * As an example, if we had settings.index.number_of_replicas, we need to make it settings.number_of_replicas.
     */ 
    private void removeIntermediateIndexSettingsLevel(ObjectNode root) {
        // Remove the intermediate key "index" under "settings", will start like:
        // {"index":{"number_of_shards":"1","number_of_replicas":"1"}}
        if (root.has("settings")) {
            ObjectNode settingsRoot = (ObjectNode) root.get("settings");
            if (settingsRoot.has("index")) {
                ObjectNode indexSettingsRoot = (ObjectNode) settingsRoot.get("index");
                settingsRoot.setAll(indexSettingsRoot);
                settingsRoot.remove("index");
                root.set("settings", settingsRoot);
            }
        }
    }

    /**
     * If allocation awareness is enabled, we need to ensure that the number of copies of our data matches the dimensionality.
     * As a specific example, if you spin up a cluster spread across 3 availability zones and your awareness attribute is "zone",
     * then the dimensionality would be 3.  This means you need to ensure the number of total copies is a multiple of 3, with
     * the minimum number of replicas being 2.
     */
    private void fixNumberOfReplicas(ObjectNode root) {
        if (root.has("settings")) {
            ObjectNode settingsRoot = (ObjectNode) root.get("settings");
            if (settingsRoot.has("number_of_replicas")) {
                // If the total number of copies requested in the original settings is not a multiple of the
                // dimensionality, then up it to the next largest multiple of the dimensionality.
                int numberOfCopies = settingsRoot.get("number_of_replicas").asInt() + 1;
                int remainder = numberOfCopies % awarenessAttributeDimensionality;
                int newNumberOfCopies = (remainder > 0) ? (numberOfCopies + awarenessAttributeDimensionality - remainder) : numberOfCopies;
                int newNumberOfReplicas = newNumberOfCopies - 1;
                settingsRoot.put("number_of_replicas", newNumberOfReplicas);
            }
        }
    }
    
}
