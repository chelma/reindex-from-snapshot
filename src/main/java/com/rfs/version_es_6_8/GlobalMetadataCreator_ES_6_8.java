package com.rfs.version_es_6_8;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rfs.common.ConnectionDetails;
import com.rfs.common.RestClient;

public class GlobalMetadataCreator_ES_6_8 {
    public static void create(ObjectNode root, ConnectionDetails connectionDetails, String[] templateWhitelist) throws Exception {
        System.out.println("Setting Global Metadata");

        GlobalMetadataData_ES_6_8 globalMetadata = new GlobalMetadataData_ES_6_8(root);
        createTemplates(globalMetadata, connectionDetails, templateWhitelist);
    }

    public static void createTemplates(GlobalMetadataData_ES_6_8 globalMetadata, ConnectionDetails connectionDetails, String[] templateWhitelist) throws Exception {
        System.out.println("Setting Templates");

        if (templateWhitelist != null) {
            for (String templateName : templateWhitelist) {
                System.out.println("Setting Template: " + templateName);
                ObjectNode settings = (ObjectNode) globalMetadata.getTemplates().get(templateName);
                createTemplate(templateName, settings, connectionDetails);
            }
        } else {
            // Get the template names
            ObjectNode templates = globalMetadata.getTemplates();
            List<String> templateKeys = new ArrayList<>();
            templates.fieldNames().forEachRemaining(templateKeys::add);

            // Create each template
            for (String templateName : templateKeys) {
                System.out.println("Setting Template: " + templateName);
                ObjectNode settings = (ObjectNode) templates.get(templateName);
                createTemplate(templateName, settings, connectionDetails);
            }
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
