package com.rfs;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class GlobalMetadataCreator {
    public static void create(GlobalMetadataProvider globalMetadataProvider, ConnectionDetails connectionDetails) throws Exception {
        System.out.println("Setting Global Metadata");
        createComponentTemplates(globalMetadataProvider, connectionDetails);
        createIndexTemplates(globalMetadataProvider, connectionDetails);
    }

    public static void createComponentTemplates(GlobalMetadataProvider globalMetadataProvider, ConnectionDetails connectionDetails) throws Exception {
        System.out.println("Setting Component Templates");

        // Get the component template names
        ObjectNode componentTemplates = globalMetadataProvider.getComponentTemplates();
        List<String> componentKeys = new ArrayList<>();
        componentTemplates.fieldNames().forEachRemaining(componentKeys::add);

        // Create each component template
        for (String templateName : componentKeys) {
            System.out.println("Setting Component Template: " + templateName);
            ObjectNode settings = (ObjectNode) componentTemplates.get(templateName);
            createComponentTemplate(templateName, settings, connectionDetails);
        }
    }

    private static void createComponentTemplate(String templateName, ObjectNode settings, ConnectionDetails connectionDetails) throws Exception {
        // Assemble the request details
        String path = "_component_template/" + templateName;
        String body = settings.toString();

        // Send the request
        RestClient client = new RestClient(connectionDetails);
        client.put(path, body);
    }

    public static void createIndexTemplates(GlobalMetadataProvider globalMetadataProvider, ConnectionDetails connectionDetails) throws Exception {
        System.out.println("Setting Index Templates");

        // Get the component template names
        ObjectNode indexTemplates = globalMetadataProvider.getIndexTemplates();
        List<String> indexKeys = new ArrayList<>();
        indexTemplates.fieldNames().forEachRemaining(indexKeys::add);

        // Create each component template
        for (String templateName : indexKeys) {
            System.out.println("Setting Index Template: " + templateName);
            ObjectNode settings = (ObjectNode) indexTemplates.get(templateName);
            createIndexTemplate(templateName, settings, connectionDetails);
        }
    }

    private static void createIndexTemplate(String templateName, ObjectNode settings, ConnectionDetails connectionDetails) throws Exception {
        // Assemble the request details
        String path = "_index_template/" + templateName;
        String body = settings.toString();

        // Send the request
        RestClient client = new RestClient(connectionDetails);
        client.put(path, body);
    }
}
