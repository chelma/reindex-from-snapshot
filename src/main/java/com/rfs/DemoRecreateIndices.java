package com.rfs;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DemoRecreateIndices {
    public static void main(String[] args) {
        // Constants
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
            SnapshotMetadataProvider snapshotMetadataProvider = SnapshotMetadataProvider.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);
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
            List<IndexMetadataProvider> indexMetadatas = new ArrayList<>();
            for (SnapshotRepoData.Index index : repoDataProvider.getIndicesInSnapshot(snapshotName)) {
                System.out.println("Reading Index Metadata for index: " + index.name);
                IndexMetadataProvider indexMetadataProvider = IndexMetadataProvider.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName, index.name);
                indexMetadatas.add(indexMetadataProvider);
            }
            System.out.println("Index Metadata read successfully");

            // ==========================================================================================================
            // Recreate the indices
            // ==========================================================================================================
            
            ConnectionDetails connectionDetails = new ConnectionDetails("localhost", 9200, "elastic-admin", "elastic-password");
            for (IndexMetadataProvider indexMetadata : indexMetadatas) {
                IndexCreator.createIndex(indexMetadata.getName() + "_reindexed", indexMetadata, connectionDetails);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
