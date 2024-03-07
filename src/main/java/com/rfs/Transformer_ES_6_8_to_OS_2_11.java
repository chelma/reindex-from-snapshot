package com.rfs;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class Transformer_ES_6_8_to_OS_2_11 implements Transformer {
    private int awarenessAttributeDimensionality;

    public Transformer_ES_6_8_to_OS_2_11(int awarenessAttributeDimensionality) {
        this.awarenessAttributeDimensionality = awarenessAttributeDimensionality;
    }

    public ObjectNode transformTemplateBody(ObjectNode root) {

        System.out.println("Original Object: " + root.toString());
        removeIntermediateMappingsLevel(root);
        fixNumberOfReplicas(root);
        System.out.println("Transformed Object: " + root.toString());
        
        return root;
    }

    public ObjectNode transformIndexBody(ObjectNode root) {
        System.out.println("Original Object: " + root.toString());
        removeIntermediateMappingsLevel(root);
        removeIntermediateIndexSettingsLevel(root); // run before fixNumberOfReplicas
        fixNumberOfReplicas(root);
        System.out.println("Transformed Object: " + root.toString());     
        
        return root;
    }

    /**
     * If the object has mappings, then we need to ensure they aren't burried underneath an intermediate key.
     * As an example, if we had mappings._doc.properties, we need to make it mappings.properties.
     */ 
    private void removeIntermediateMappingsLevel(ObjectNode root) {        
        if (root.has("mappings")) {
            ObjectNode mappingsRoot = (ObjectNode) root.get("mappings");

            String[] subKeys = new String[] {"_doc", "doc"};
            for (String subKey : subKeys) {
                if (mappingsRoot.has(subKey)) {
                    ObjectNode subNode = (ObjectNode) mappingsRoot.get(subKey);

                    subNode.fieldNames().forEachRemaining(fieldName -> {
                        mappingsRoot.set(fieldName, subNode.get(fieldName));
                    });

                    mappingsRoot.remove(subKey);
                }
            }
        }
    }
    /**
     * If the object has settings, then we need to ensure they aren't burried underneath an intermediate key.
     * As an example, if we had settings.index.number_of_replicas, we need to make it settings.number_of_replicas.
     */ 
    private void removeIntermediateIndexSettingsLevel(ObjectNode root) {        
        if (root.has("settings")) {
            ObjectNode mappingsRoot = (ObjectNode) root.get("settings");

            String[] subKeys = new String[] {"index"};
            for (String subKey : subKeys) {
                if (mappingsRoot.has(subKey)) {
                    ObjectNode subNode = (ObjectNode) mappingsRoot.get(subKey);

                    subNode.fieldNames().forEachRemaining(fieldName -> {
                        mappingsRoot.set(fieldName, subNode.get(fieldName));
                    });

                    mappingsRoot.remove(subKey);
                }
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
