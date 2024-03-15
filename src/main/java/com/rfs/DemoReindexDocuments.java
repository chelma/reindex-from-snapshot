package com.rfs;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;


public class DemoReindexDocuments {
    public static void main(String[] args) {
        if (args.length < 6) {
            System.out.println("Usage: java DemoReindexDocuments "
                                + " <snapshot name> <absolute path to snapshot directory> <absolute path to dir where we'll put the lucene docs>"
                                + " <target host and port (e.g. http://localhost:9200)> <target username> <target password>");
            return;
        }

        String snapshotName = args[0];
        String snapshotDirPath = args[1];
        String luceneBasePathString = args[2];
        String targetHost = args[3];
        String targetUser = args[4];
        String targetPass = args[5];
        Path luceneBasePath = Paths.get(luceneBasePathString);
        ConnectionDetails targetConnection = new ConnectionDetails(targetHost, targetUser, targetPass);

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
            SnapshotMetadata snapshotMetadata = SnapshotMetadataProvider.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);
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
                    SnapshotShardUnpacker.unpack(shardMetadata, luceneBasePath);                    
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

                    List<Document> documents = LuceneDocumentsReader.readDocuments(luceneBasePath, indexMetadata.getName(), shardId);
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
