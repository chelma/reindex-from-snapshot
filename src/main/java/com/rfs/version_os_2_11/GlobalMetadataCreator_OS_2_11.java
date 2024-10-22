package com.rfs.version_os_2_11;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rfs.common.ConnectionDetails;
import com.rfs.common.RestClient;

public class GlobalMetadataCreator_OS_2_11 {
    public static void create(ObjectNode root, ConnectionDetails connectionDetails, String[] componentTemplateWhitelist, String[] indexTemplateWhitelist) throws Exception {
        System.out.println("Setting Global Metadata");

        GlobalMetadataData_OS_2_11 globalMetadata = new GlobalMetadataData_OS_2_11(root);
        createTemplates(globalMetadata, connectionDetails, indexTemplateWhitelist);
        createComponentTemplates(globalMetadata, connectionDetails, componentTemplateWhitelist);
        createIndexTemplates(globalMetadata, connectionDetails, indexTemplateWhitelist);
    }

    public static void createTemplates(GlobalMetadataData_OS_2_11 globalMetadata, ConnectionDetails connectionDetails, String[] indexTemplateWhitelist) throws Exception {
        System.out.println("Setting Legacy Templates");
        ObjectNode templates = globalMetadata.getTemplates();

        if (templates == null){
            System.out.println("No Legacy Templates");
            return;
        }

        if (indexTemplateWhitelist != null) {
            for (String templateName : indexTemplateWhitelist) {
                if (!templates.has(templateName) || templates.get(templateName) == null) {
                    System.out.println("Legacy Template not found: " + templateName);
                    continue;
                }

                System.out.println("Setting Legacy Template: " + templateName);
                ObjectNode settings = (ObjectNode) globalMetadata.getTemplates().get(templateName);
                System.out.println("Legacy Template Settings: " + settings);
                String path = "_template/" + templateName;
                createEntity(templateName, settings, connectionDetails, path);
            }
        } else {
            // Get the template names
            List<String> templateKeys = new ArrayList<>();
            templates.fieldNames().forEachRemaining(templateKeys::add);

            // Create each template
            for (String templateName : templateKeys) {
                System.out.println("Setting Legacy Template: " + templateName);
                ObjectNode settings = (ObjectNode) templates.get(templateName);
                String path = "_template/" + templateName;
                createEntity(templateName, settings, connectionDetails, path);
            }
        }
    }

    public static void createComponentTemplates(GlobalMetadataData_OS_2_11 globalMetadata, ConnectionDetails connectionDetails, String[] indexTemplateWhitelist) throws Exception {
        System.out.println("Setting Component Templates");
        ObjectNode templates = globalMetadata.getComponentTemplates();

        if (templates == null){
            System.out.println("No Component Templates");
            return;
        }

        if (indexTemplateWhitelist != null) {            
            for (String templateName : indexTemplateWhitelist) {
                if (!templates.has(templateName) || templates.get(templateName) == null) {
                    System.out.println("Component Template not found: " + templateName);
                    continue;
                }

                System.out.println("Setting Component Template: " + templateName);
                ObjectNode settings = (ObjectNode) templates.get(templateName);
                String path = "_component_template/" + templateName;
                createEntity(templateName, settings, connectionDetails, path);
            }
        } else {
            // Get the template names
            List<String> templateKeys = new ArrayList<>();
            templates.fieldNames().forEachRemaining(templateKeys::add);

            // Create each template
            for (String templateName : templateKeys) {
                System.out.println("Setting Component Template: " + templateName);
                ObjectNode settings = (ObjectNode) templates.get(templateName);
                String path = "_component_template/" + templateName;
                createEntity(templateName, settings, connectionDetails, path);
            }
        }
    }

    public static void createIndexTemplates(GlobalMetadataData_OS_2_11 globalMetadata, ConnectionDetails connectionDetails, String[] indexTemplateWhitelist) throws Exception {
        System.out.println("Setting Index Templates");
        ObjectNode templates = globalMetadata.getIndexTemplates();

        if (templates == null){
            System.out.println("No Index Templates");
            return;
        }

        if (indexTemplateWhitelist != null) {
            for (String templateName : indexTemplateWhitelist) {
                if (!templates.has(templateName) || templates.get(templateName) == null) {
                    System.out.println("Index Template not found: " + templateName);
                    continue;
                }

                System.out.println("Setting Index Template: " + templateName);
                ObjectNode settings = (ObjectNode) globalMetadata.getIndexTemplates().get(templateName);
                String path = "_index_template/" + templateName;
                createEntity(templateName, settings, connectionDetails, path);
            }
        } else {
            // Get the template names
            List<String> templateKeys = new ArrayList<>();
            templates.fieldNames().forEachRemaining(templateKeys::add);

            // Create each template
            for (String templateName : templateKeys) {
                System.out.println("Setting Index Template: " + templateName);
                ObjectNode settings = (ObjectNode) templates.get(templateName);
                String path = "_index_template/" + templateName;
                createEntity(templateName, settings, connectionDetails, path);
            }
        }
    }

    private static void createEntity(String entityName, ObjectNode settings, ConnectionDetails connectionDetails, String path) throws Exception {
        // Assemble the request details
        String body = settings.toString();

        // Send the request
        RestClient client = new RestClient(connectionDetails);
        client.put(path, body);
    }
}
