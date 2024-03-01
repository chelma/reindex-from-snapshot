package com.rfs;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.cluster.metadata.ComponentTemplateMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
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
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;

public class DemoRecreateIndices {
    public static void main(String[] args) {
        // Constants
        int bufferSizeInBytes = 131072; // Magic number pulled from running ES process

        // Paths
        String snapshotName = "global_state_snapshot";
        String snapshotDirPath = "/Users/chelma/workspace/ElasticSearch/elasticsearch/build/testclusters/runTask-0/repo/snapshots";        

        try {
            // ==========================================================================================================
            // Read the Repo data file
            // ==========================================================================================================
            System.out.println("Attempting to read Repo data file...");
            SnapshotRepoDataProvider repoDataProvider = new SnapshotRepoDataProvider(Paths.get(snapshotDirPath));
            System.out.println("Repo data read successfully");

            // ==========================================================================================================
            // Read the Snapshot details
            // ==========================================================================================================
            System.out.println("Attempting to read Snapshot details...");
            String snapshotIdString = repoDataProvider.getSnapshotId(snapshotName);

            if (snapshotIdString == null) {
                System.out.println("Snapshot not found");
                return;
            }
            SnapshotId snapshotId = new SnapshotId(snapshotName, snapshotIdString);
            SnapshotInfo snapshotInfo = readSnapshotDetails(snapshotDirPath, snapshotId);
            System.out.println("Snapshot data read successfully");

            // ==========================================================================================================
            // Read the Global Metadata
            // ==========================================================================================================
            System.out.println("Attempting to read Global Metadata details...");
            GlobalMetadataProvider globalMetadataProvider = GlobalMetadataProvider.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);
            System.out.println("Global Metadata read successfully");

            // ==========================================================================================================
            // Read all the Index Metadata
            // ==========================================================================================================
            System.out.println("Attempting to read Index Metadata...");
            List<IndexMetadata> indexMetadatas = new ArrayList<>();
            for (SnapshotRepoData.Index index : repoDataProvider.getIndicesInSnapshot(snapshotName)) {
                System.out.println("Reading Index Metadata for index: " + index.name);
                String snapshotIndexDirPath = snapshotDirPath + "/indices/" + index.id;
                
                String indexMetadataId = repoDataProvider.getIndexMetadataId(snapshotName, index.name);

                IndexMetadata indexMetadata = readIndexMetadata(snapshotDirPath, snapshotIndexDirPath, indexMetadataId);
                indexMetadatas.add(indexMetadata);
            }
            System.out.println("Index Metadata read successfully");

            // ==========================================================================================================
            // Recreate the indices
            // ==========================================================================================================
            
            ConnectionDetails connectionDetails = new ConnectionDetails("localhost", 9200, "elastic-admin", "elastic-password");
            for (IndexMetadata indexMetadata : indexMetadatas) {
                IndexCreator.createIndex(indexMetadata.getIndex().getName() + "_reindexed", indexMetadata, connectionDetails);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static SnapshotInfo readSnapshotDetails(String snapshotDirPath, SnapshotId snapshotId) throws Exception {
        Path snapshotPath = Paths.get(snapshotDirPath);
        BlobPath blobPath = new BlobPath();
        blobPath.add(snapshotDirPath);

        FsBlobStore blobStore = new FsBlobStore(
            131072, // Magic number pulled from running ES process
            snapshotPath, 
            false
        );
        BlobContainer container = blobStore.blobContainer(blobPath);

        ChecksumBlobStoreFormat<SnapshotInfo> snapshotFormat =
            new ChecksumBlobStoreFormat<>("snapshot", "snap-%s.dat", SnapshotInfo::fromXContentInternal);

        // Make an empty registry; may not work for all cases?
        List<NamedXContentRegistry.Entry> entries = List.of();
        NamedXContentRegistry registry = new NamedXContentRegistry(entries);

        // Read the snapshot details
        SnapshotInfo snapshotInfo = snapshotFormat.read(container, snapshotId.getUUID(), registry);

        blobStore.close();

        return snapshotInfo;
    }

    private static IndexMetadata readIndexMetadata(String snapshotDirPath, String snapshotIndexDirPath, String indexMetadataId) throws Exception {
        Path snapshotPath = Paths.get(snapshotDirPath);
        BlobPath blobPath = new BlobPath().add(snapshotIndexDirPath);

        FsBlobStore blobStore = new FsBlobStore(
            131072, // Magic number pulled from running ES process
            snapshotPath, 
            false
        );
        BlobContainer container = blobStore.blobContainer(blobPath);

        ChecksumBlobStoreFormat<IndexMetadata> indexMetadataFormat =
            new ChecksumBlobStoreFormat<>("index-metadata", "meta-%s.dat", IndexMetadata::fromXContent);

        // Make an empty registry; may not work for all cases?
        List<NamedXContentRegistry.Entry> entries = List.of();
        NamedXContentRegistry registry = new NamedXContentRegistry(entries);
        
        IndexMetadata indexMetadata = indexMetadataFormat.read(container, indexMetadataId, registry);

        blobStore.close();

        return indexMetadata;
    }    
}
