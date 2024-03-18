package com.rfs;

import org.apache.lucene.document.Document;

public class DocumentReindexer {
    public static void reindex(String indexName, Document document, ConnectionDetails targetConnection) throws Exception {

        // List<org.apache.lucene.index.IndexableField> fields = document.getFields();
        // for (org.apache.lucene.index.IndexableField field : fields) {
        //     if ("_source".equals(field.name())){
        //         String source_string = document.getBinaryValue("_source").utf8ToString();
        //         System.out.println("Field name: " + field.name() + ", Field value: " + source_string);
        //     } else if ("_id".equals(field.name())){
        //         String uid = Uid.decodeId(document.getBinaryValue(field.name()).bytes);
        //         System.out.println("Field name: " + field.name() + ", Field value: " + uid);
        //     } else {
        //         System.out.println("Field name: " + field.name());
        //     }
        // }
        
        // Get the document details
        String id = Uid.decodeId(document.getBinaryValue("_id").bytes);
        String source = document.getBinaryValue("_source").utf8ToString();


        System.out.println("Reindexing document - Index: " + indexName + ", Document ID: " + id);

        // Assemble the request details
        String path = indexName + "/_doc/" + id;
        String body = source;

        // Send the request
        RestClient client = new RestClient(targetConnection);
        client.put(path, body);
    }
}
