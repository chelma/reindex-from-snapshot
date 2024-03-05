package com.rfs;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class IndexMetadataProvider {
    private final String indexId;
    private final IndexMetadata indexMetadata;

    public static IndexMetadataProvider fromSnapshotRepoDataProvider(SnapshotRepoDataProvider repoDataProvider, String snapshotName, String indexName) throws Exception{
        String indexId = repoDataProvider.getIndexId(indexName);
        Path indexDirPath = Paths.get(repoDataProvider.getSnapshotDirPath() + "/indices/" + indexId);
        String indexMetadataId = repoDataProvider.getIndexMetadataId(snapshotName, indexName);

        IndexMetadata indexMetadata = readIndexMetadata(repoDataProvider.getSnapshotDirPath(), indexDirPath, indexMetadataId);
        return new IndexMetadataProvider(indexId, indexMetadata);
    }

    public IndexMetadataProvider(String indexId, IndexMetadata indexMetadata) {
        this.indexId = indexId;
        this.indexMetadata = indexMetadata;
    }

    private static IndexMetadata readIndexMetadata(Path snapshotDirPath, Path indexDirPath, String indexMetadataId) throws Exception {
        BlobPath blobPath = new BlobPath().add(indexDirPath.toString());

        FsBlobStore blobStore = new FsBlobStore(
            ElasticsearchConstants.BUFFER_SIZE_IN_BYTES,
            snapshotDirPath, 
            false
        );
        BlobContainer container = blobStore.blobContainer(blobPath);

        ChecksumBlobStoreFormat<IndexMetadata> indexMetadataFormat =
            new ChecksumBlobStoreFormat<>("index-metadata", "meta-%s.dat", IndexMetadata::fromXContent);
        
        IndexMetadata indexMetadata = indexMetadataFormat.read(container, indexMetadataId, ElasticsearchConstants.EMPTY_REGISTRY);

        blobStore.close();

        return indexMetadata;
    }

    public String getId() {
        return indexId;
    }

    public String getName() {
        return indexMetadata.getIndex().getName();
    }

    public int getNumberOfShards() {
        return indexMetadata.getNumberOfShards();
    }

    public ObjectNode getMappingsJson() throws Exception {
        // Will be something like:
        // {"_doc":{"properties":{"address":{"type":"text"}}}}
        String rawMetadata = indexMetadata.mapping().source().toString();

        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootJson = mapper.readTree(rawMetadata);
        ObjectNode root = (ObjectNode) rootJson;
        
        return (ObjectNode) root.get("_doc");
    }

    public ObjectNode getAliasesJson() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        for (ObjectCursor<String> aliasName : indexMetadata.getAliases().keys()) {
            AliasMetadata aliasMetadata = indexMetadata.getAliases().get(aliasName.value);
            String aliasString = aliasMetadata.toString();
            JsonNode rootJson = mapper.readTree(aliasString);
            root.set(aliasName.value, (ObjectNode) rootJson.get(aliasName.value));
        }
        
        return root;
    }

    public ObjectNode getSettingsJson() {
        // Will be something like:
        // index.creation_date=1709172482837,index.number_of_replicas=1,index.number_of_shards=1,index.provided_name=index_2,
        // index.routing.allocation.include._tier_preference=data_content,index.uuid=a1YVzezrRpCw_XiAW7yyLg,index.version.created=7100299,
        String rawSettings = indexMetadata.getSettings().toDelimitedString(',');
        String[] pairs = rawSettings.split(",");

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();

        for (String pair : pairs) {
            String[] keyValue = pair.split("=");
            String key = keyValue[0];
            String value = keyValue[1];
            
            String[] parts = key.split("\\.");
            ObjectNode current = root;
            
            for (int i = 0; i < parts.length - 1; i++) {
                String part = parts[i];
                if (!current.has(part)) {
                    current.set(part, mapper.createObjectNode());
                }
                current = (ObjectNode) current.get(part);
            }
            
            current.put(parts[parts.length - 1], value);
        }
        
        return root;
    }
}
