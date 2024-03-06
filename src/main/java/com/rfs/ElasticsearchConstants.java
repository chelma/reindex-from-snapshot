package com.rfs;

import java.util.ArrayList;
import java.util.List;


import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.script.ScriptMetaData;


public class ElasticsearchConstants {
    public static final int BUFFER_SIZE_IN_BYTES;
    public static final Settings BUFFER_SETTINGS;
    public static final  NamedXContentRegistry EMPTY_REGISTRY;
    public static final  NamedXContentRegistry GLOBAL_METADATA_REGISTRY;

    static {
        // https://github.com/elastic/elasticsearch/blob/6.8/server/src/main/java/org/elasticsearch/common/blobstore/fs/FsBlobStore.java#L49
        BUFFER_SIZE_IN_BYTES = 102400; // Default buffer size
        BUFFER_SETTINGS = Settings.builder().put("repositories.fs.buffer_size", "100kb").build();

        // Make an empty registry; may not work for all cases?
        EMPTY_REGISTRY = new NamedXContentRegistry(new ArrayList<>());

        // Pulled from https://github.com/elastic/elasticsearch/blob/v6.8.23/server/src/main/java/org/elasticsearch/cluster/ClusterModule.java#L183
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.add(new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(RepositoriesMetaData.TYPE),
            RepositoriesMetaData::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(IngestMetadata.TYPE),
            IngestMetadata::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(ScriptMetaData.TYPE),
            ScriptMetaData::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(IndexGraveyard.TYPE),
            IndexGraveyard::fromXContent));
        entries.add(new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(PersistentTasksCustomMetaData.TYPE),
            PersistentTasksCustomMetaData::fromXContent));
        GLOBAL_METADATA_REGISTRY = new NamedXContentRegistry(entries);
    }
    
}
