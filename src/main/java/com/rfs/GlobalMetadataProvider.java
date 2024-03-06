package com.rfs;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class GlobalMetadataProvider {
    private final MetaData globalMetadata;

    public static GlobalMetadataProvider fromSnapshotRepoDataProvider(SnapshotRepoDataProvider repoDataProvider, String snapshotName) throws Exception {
        MetaData globalMetadata = readGlobalMetadata(repoDataProvider.getSnapshotDirPath(), repoDataProvider.getSnapshotId(snapshotName));

        return new GlobalMetadataProvider(globalMetadata);
    }

    private static MetaData readGlobalMetadata(Path snapshotDirPath, String snapshotId) throws Exception {
        BlobPath blobPath = new BlobPath();
        blobPath.add(snapshotDirPath.toString());

        FsBlobStore blobStore = new FsBlobStore(
            ElasticsearchConstants.BUFFER_SETTINGS,
            snapshotDirPath, 
            false
        );
        BlobContainer container = blobStore.blobContainer(blobPath);

        // See https://github.com/elastic/elasticsearch/blob/6.8/server/src/main/java/org/elasticsearch/repositories/blobstore/BlobStoreRepository.java#L353
        boolean compressionEnabled = false;

        ChecksumBlobStoreFormat<MetaData> global_metadata_format =
            new ChecksumBlobStoreFormat<>("metadata", "meta-%s.dat", MetaData::fromXContent, ElasticsearchConstants.GLOBAL_METADATA_REGISTRY, compressionEnabled);

        // Read the snapshot details
        MetaData globalMetadata = global_metadata_format.read(container, snapshotId);

        blobStore.close();

        return globalMetadata;
    }

    public GlobalMetadataProvider(MetaData globalMetadata) {
        this.globalMetadata = globalMetadata;
    }

    public ObjectNode getTemplates() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();


        List<String> keyNames = new ArrayList<>();
        for (ObjectCursor<String> keyCursor : globalMetadata.templates().keys()) {
            keyNames.add(keyCursor.value);
        }

        for (String key : keyNames) {
            IndexTemplateMetaData template = globalMetadata.templates().get(key);

            ObjectNode templateRoot = mapper.createObjectNode();

            // Construct the alias tree structure
            ObjectNode aliasRoot = mapper.createObjectNode();
            for (ObjectCursor<String> aliasCursor : template.getAliases().keys()) {
                String aliasName = aliasCursor.value;
                JsonNode aliasJson = mapper.readTree(template.getAliases().get(aliasName).toString());
                aliasRoot.set(aliasName, (ObjectNode) aliasJson);
            }
            if (aliasRoot.size() > 0){
                templateRoot.set("aliases", aliasRoot);
            }

            // Construct the mappings tree structure
            ObjectNode mappingsRoot = mapper.createObjectNode();
            for (ObjectCursor<String> mappingCursor : template.getMappings().keys()) {
                String mappingName = mappingCursor.value;
                JsonNode mappingJson = mapper.readTree(template.getMappings().get(mappingName).toString());
                mappingsRoot.set(mappingName, (ObjectNode) mappingJson.get(mappingName));
            }
            if (mappingsRoot.size() > 0){
                templateRoot.set("mappings", mappingsRoot);
            }

            // Construct the patterns tree structure
            ArrayNode patternsRoot = mapper.createArrayNode();
            template.getPatterns().forEach(patternsRoot::add);
            if (patternsRoot.size() > 0){
                templateRoot.set("index_patterns", patternsRoot);
            }            

            // Construct the settings tree structure
            JsonNode flatNode = mapper.readTree(template.getSettings().toString());
            ObjectNode settingsRoot = mapper.createObjectNode();
            
            flatNode.fields().forEachRemaining(entry -> {
                String[] keys = entry.getKey().split("\\.");
                ObjectNode current = settingsRoot;
                
                for (int i = 0; i < keys.length - 1; i++) {
                    if (!current.has(keys[i])) {
                        current.set(keys[i], mapper.createObjectNode());
                    }
                    current = (ObjectNode) current.get(keys[i]);
                }
                
                current.set(keys[keys.length - 1], entry.getValue());
            });
            if (settingsRoot.size() > 0){
                templateRoot.set("settings", (ObjectNode) settingsRoot.get("index"));
            }

            root.set(key, templateRoot);
        }

        return root;
    }    
}
