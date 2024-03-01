package com.rfs;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComponentTemplateMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.elasticsearch.script.ScriptMetadata;

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
            131072, // Magic number pulled from running ES process
            snapshotDirPath, 
            false
        );
        BlobContainer container = blobStore.blobContainer(blobPath);

        ChecksumBlobStoreFormat<Metadata> global_metadata_format =
            new ChecksumBlobStoreFormat<>("metadata", "meta-%s.dat", Metadata::fromXContent);

        // Pulled from https://github.com/elastic/elasticsearch/blob/v7.10.2/server/src/main/java/org/elasticsearch/cluster/ClusterModule.java#L180
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(RepositoriesMetadata.TYPE),
            RepositoriesMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(IngestMetadata.TYPE),
            IngestMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(ScriptMetadata.TYPE),
            ScriptMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(IndexGraveyard.TYPE),
            IndexGraveyard::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(PersistentTasksCustomMetadata.TYPE),
            PersistentTasksCustomMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(ComponentTemplateMetadata.TYPE),
            ComponentTemplateMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(ComposableIndexTemplateMetadata.TYPE),
            ComposableIndexTemplateMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(DataStreamMetadata.TYPE),
            DataStreamMetadata::fromXContent));
        NamedXContentRegistry registry = new NamedXContentRegistry(entries);

        // Read the snapshot details
        Metadata globalMetadata = global_metadata_format.read(container, snapshotId, registry);

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
