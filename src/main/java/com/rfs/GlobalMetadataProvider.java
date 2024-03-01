package com.rfs;

import java.nio.file.Path;

import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class GlobalMetadataProvider {
    private final Metadata globalMetadata;

    public static GlobalMetadataProvider fromSnapshotRepoDataProvider(SnapshotRepoDataProvider repoDataProvider, String snapshotName) throws Exception {
        Metadata globalMetadata = readGlobalMetadata(repoDataProvider.getSnapshotDirPath(), repoDataProvider.getSnapshotId(snapshotName));

        return new GlobalMetadataProvider(globalMetadata);
    }

    private static Metadata readGlobalMetadata(Path snapshotDirPath, String snapshotId) throws Exception {
        BlobPath blobPath = new BlobPath();
        blobPath.add(snapshotDirPath.toString());

        FsBlobStore blobStore = new FsBlobStore(
            ElasticsearchConstants.BUFFER_SIZE_IN_BYTES,
            snapshotDirPath, 
            false
        );
        BlobContainer container = blobStore.blobContainer(blobPath);

        ChecksumBlobStoreFormat<Metadata> global_metadata_format =
            new ChecksumBlobStoreFormat<>("metadata", "meta-%s.dat", Metadata::fromXContent);

        // Read the snapshot details
        Metadata globalMetadata = global_metadata_format.read(container, snapshotId, ElasticsearchConstants.GLOBAL_METADATA_REGISTRY);

        blobStore.close();

        return globalMetadata;
    }

    public GlobalMetadataProvider(Metadata globalMetadata) {
        this.globalMetadata = globalMetadata;
    }

    public ObjectNode getComponentTemplates() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();

        for (String key : globalMetadata.componentTemplates().keySet()) {
            ComponentTemplate componentTemplate = globalMetadata.componentTemplates().get(key);
            String componentTemplateString = componentTemplate.toString();
            JsonNode componentJson = mapper.readTree(componentTemplateString);
            root.set(key, (ObjectNode) componentJson);
        }

        return root;
    }

    public ObjectNode getIndexTemplates() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();

        for (String key : globalMetadata.templatesV2().keySet()) {
            ComposableIndexTemplate indexTemplate = globalMetadata.templatesV2().get(key);
            String indexTemplateString = indexTemplate.toString();
            JsonNode indexJson = mapper.readTree(indexTemplateString);
            root.set(key, (ObjectNode) indexJson);
        }

        return root;
    }
    
}
