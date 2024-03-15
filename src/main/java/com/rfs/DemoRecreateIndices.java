package com.rfs;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DemoRecreateIndices {
    public static void main(String[] args) {
        if (args.length < 5) {
            System.out.println("Usage: java DemoRecreateIndices "
                                + " <snapshot name> <absolute path to snapshot directory> "
                                + " <target host and port (e.g. http://localhost:9200)> <target username> <target password>");
            return;
        }

        String snapshotName = args[0];
        String snapshotDirPath = args[1];
        String targetHost = args[2];
        String targetUser = args[3];
        String targetPass = args[4];
        ConnectionDetails targetConnection = new ConnectionDetails(targetHost, targetUser, targetPass);

        // Should probably be passed in as an argument
        String[] templateWhitelist = {"posts_index_template"};

        // Should determine the source, target versions programmatically.  The dimensionality is from the target I'm using
        // to test.
        Transformer_ES_6_8_to_OS_2_11 transformer = new Transformer_ES_6_8_to_OS_2_11(3);

        try {
            // ==========================================================================================================
            // Read the Repo data file
            // ==========================================================================================================
            System.out.println("Attempting to read Repo data file...");
            SnapshotRepoDataProvider repoDataProvider = new SnapshotRepoDataProvider(Paths.get(snapshotDirPath));
            System.out.println("Repo data read successfully");

            // // ==========================================================================================================
            // // Read the Snapshot details
            // // ==========================================================================================================
            System.out.println("Attempting to read Snapshot details...");
            String snapshotIdString = repoDataProvider.getSnapshotId(snapshotName);

            if (snapshotIdString == null) {
                System.out.println("Snapshot not found");
                return;
            }
            SnapshotMetadata snapshotMetadata = SnapshotMetadataProvider.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);
            System.out.println("Snapshot data read successfully");

            // ==========================================================================================================
            // Read the Global Metadata
            // ==========================================================================================================
            System.out.println("Attempting to read Global Metadata details...");
            GlobalMetadataProvider globalMetadataProvider = GlobalMetadataProvider.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);
            System.out.println("Global Metadata read successfully");

            // ==========================================================================================================
            // Recreate the Global Metadata
            // ==========================================================================================================
            System.out.println("Attempting to recreate the Global Metadata...");
            GlobalMetadataCreator.create(globalMetadataProvider, targetConnection, templateWhitelist, transformer);

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
            System.out.println("Attempting to recreate the indices...");
            for (IndexMetadataProvider indexMetadata : indexMetadatas) {
                String reindexName = indexMetadata.getName() + "_reindexed";
                System.out.println("Recreating index " + indexMetadata.getName() + " as " + reindexName + " on target...");
                IndexCreator.create(reindexName, indexMetadata, targetConnection, transformer);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
