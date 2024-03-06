package com.rfs;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class GlobalMetadataCreator {
    public static void create(GlobalMetadataProvider globalMetadataProvider, ConnectionDetails connectionDetails) throws Exception {
        System.out.println("Setting Global Metadata");
        createTemplates(globalMetadataProvider, connectionDetails);
    }

    public static void createTemplates(GlobalMetadataProvider globalMetadataProvider, ConnectionDetails connectionDetails) throws Exception {
        System.out.println("Setting Component Templates");

        // Get the component template names
        ObjectNode templates = globalMetadataProvider.getTemplates();
        List<String> templateKeys = new ArrayList<>();
        templates.fieldNames().forEachRemaining(templateKeys::add);

        // Create each component template
        for (String templateName : templateKeys) {
            System.out.println("Setting Template: " + templateName);
            ObjectNode settings = (ObjectNode) templates.get(templateName);
            createTemplate(templateName, settings, connectionDetails);
        }
    }

    private static void createTemplate(String templateName, ObjectNode settings, ConnectionDetails connectionDetails) throws Exception {
        // Assemble the request details
        String path = "_template/" + templateName;
        String body = settings.toString();

        // Send the request
        RestClient client = new RestClient(connectionDetails);
        client.put(path, body);
    }
}
