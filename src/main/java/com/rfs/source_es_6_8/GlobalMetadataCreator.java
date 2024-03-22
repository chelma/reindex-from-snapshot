package com.rfs.source_es_6_8;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rfs.common.ConnectionDetails;
import com.rfs.common.RestClient;

public class GlobalMetadataCreator {
    public static void create(GlobalMetadata globalMetadata, ConnectionDetails connectionDetails, String[] templateWhitelist, Transformer transformer) throws Exception {
        System.out.println("Setting Global Metadata");
        createTemplates(globalMetadata, connectionDetails, templateWhitelist, transformer);
    }

    public static void createTemplates(GlobalMetadata globalMetadata, ConnectionDetails connectionDetails, String[] templateWhitelist, Transformer transformer) throws Exception {
        System.out.println("Setting Component Templates");

        if (templateWhitelist != null) {
            for (String templateName : templateWhitelist) {
                System.out.println("Setting Template: " + templateName);
                ObjectNode settings = (ObjectNode) globalMetadata.getTemplates().get(templateName);

                ObjectNode transformedSettings = transformer.transformTemplateBody(settings);
                createTemplate(templateName, transformedSettings, connectionDetails);
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
                ObjectNode transformedSettings = transformer.transformTemplateBody(settings);
                createTemplate(templateName, transformedSettings, connectionDetails);
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
