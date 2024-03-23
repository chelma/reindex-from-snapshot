package com.rfs.version_es_7_10;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rfs.common.Transformer;

public class Transformer_ES_7_10_OS_2_11 implements Transformer {
    private static final ObjectMapper mapper = new ObjectMapper();
    private int awarenessAttributeDimensionality;

    public Transformer_ES_7_10_OS_2_11(int awarenessAttributeDimensionality) {
        this.awarenessAttributeDimensionality = awarenessAttributeDimensionality;
    }

    public ObjectNode transformGlobalMetadata(ObjectNode root){
        ObjectNode newRoot = mapper.createObjectNode();

        // Transform the legacy templates
        ObjectNode templatesRoot = (ObjectNode) root.get("templates").deepCopy();
        templatesRoot.fieldNames().forEachRemaining(templateName -> {
            ObjectNode template = (ObjectNode) templatesRoot.get(templateName);
            System.out.println("Transforming template: " + templateName);
            System.out.println("Original template: " + template.toString());
            removeIntermediateIndexSettingsLevel(template); // run before fixNumberOfReplicas
            fixNumberOfReplicas(template);
            System.out.println("Transformed template: " + template.toString());
            templatesRoot.set(templateName, template);
        });
        newRoot.set("templates", templatesRoot);

        // Transform the index templates
        ObjectNode indexTemplatesRoot = (ObjectNode) root.get("index_template").deepCopy();
        ObjectNode indexTemplateValuesRoot = (ObjectNode) indexTemplatesRoot.get("index_template");
        indexTemplateValuesRoot.fieldNames().forEachRemaining(templateName -> {
            ObjectNode template = (ObjectNode) indexTemplateValuesRoot.get(templateName);
            ObjectNode templateSubRoot = (ObjectNode) template.get("template");

            if (templateSubRoot == null) {
                return;
            }

            System.out.println("Transforming template: " + templateName);
            System.out.println("Original template: " + template.toString());
            removeIntermediateIndexSettingsLevel(templateSubRoot); // run before fixNumberOfReplicas
            fixNumberOfReplicas(templateSubRoot);
            System.out.println("Transformed template: " + template.toString());
            indexTemplateValuesRoot.set(templateName, template);
        });
        newRoot.set("index_template", indexTemplatesRoot);

        // Transform the component templates
        ObjectNode componentTemplatesRoot = (ObjectNode) root.get("component_template").deepCopy();
        newRoot.set("component_template", componentTemplatesRoot);

        return newRoot;
    }

    public ObjectNode transformIndexMetadata(ObjectNode root){
        ObjectNode newRoot = root.deepCopy();
        return newRoot;
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
