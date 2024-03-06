package com.rfs;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;


public class DemoReindexDocuments {
    public static void main(String[] args) {
        // Constants; will be replaced with user input
        String snapshotName = "global_state_snapshot";
        String snapshotDirPath = "/Users/chelma/workspace/ElasticSearch/elasticsearch/distribution/build/cluster/shared/repo";
        Path luceneFilesBasePath = Paths.get("/tmp/lucene_files");
        ConnectionDetails targetConnection = new ConnectionDetails("localhost", 9200, "elastic-admin", "elastic-password");

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
            // Read all the Index Metadata
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Attempting to read Index Metadata...");
            Map<String, IndexMetadataProvider> indexMetadatas = new HashMap<>();
            for (SnapshotRepoData.Index index : repoDataProvider.getIndicesInSnapshot(snapshotName)) {
                System.out.println("Reading Index Metadata for index: " + index.name);
                IndexMetadataProvider indexMetadataProvider = IndexMetadataProvider.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName, index.name);
                indexMetadatas.put(index.name, indexMetadataProvider);
            }
            System.out.println("Index Metadata read successfully");

            // ==========================================================================================================
            // Unpack the snapshot blobs
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Unpacking blob files to disk...");

            for (IndexMetadataProvider indexMetadata : indexMetadatas.values()) {
                System.out.println("Processing index: " + indexMetadata.getName());
                for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
                    System.out.println("=== Shard ID: " + shardId + " ===");

                    // Get the shard metadata
                    ShardMetadataProvider shardMetadata = ShardMetadataProvider.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName, indexMetadata.getName(), shardId);
                    SnapshotShardUnpacker.unpack(shardMetadata, luceneFilesBasePath);                    
                }
            }

            System.out.println("Blob files unpacked successfully");

            // ==========================================================================================================
            // Reindex the documents
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Reindexing the documents...");

            for (IndexMetadataProvider indexMetadata : indexMetadatas.values()) {
                for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
                    System.out.println("=== Index Id: " + indexMetadata.getName() + ", Shard ID: " + shardId + " ===");

                    List<Document> documents = LuceneDocumentsReader.readDocuments(luceneFilesBasePath, indexMetadata.getName(), shardId);
                    System.out.println("Documents read successfully");

                    for (Document document : documents) {
                        String targetIndex = indexMetadata.getName() + "_reindexed";
                        DocumentReindexer.reindex(targetIndex, document, targetConnection);
                    }
                }
            }

            System.out.println("Documents reindexed successfully");
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }    
}
