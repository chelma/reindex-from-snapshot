package com.rfs;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.cluster.metadata.ComponentTemplateMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.script.ScriptMetadata;

public class ElasticsearchConstants {
    public static final int BUFFER_SIZE_IN_BYTES = 131072; // Magic number pulled from running ES 7.10 process
    public static final  NamedXContentRegistry EMPTY_REGISTRY;
    public static final  NamedXContentRegistry GLOBAL_METADATA_REGISTRY;

    static {
        // Make an empty registry; may not work for all cases?
        EMPTY_REGISTRY = new NamedXContentRegistry(new ArrayList<>());

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
        GLOBAL_METADATA_REGISTRY = new NamedXContentRegistry(entries);
    }
    
}
