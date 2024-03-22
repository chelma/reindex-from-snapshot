package com.rfs;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.rfs.common.ConnectionDetails;
import com.rfs.common.DocumentReindexer;
import com.rfs.common.LuceneDocumentsReader;
import com.rfs.common.SourceVersion;
import com.rfs.source_es_6_8.IndexMetadata;
import com.rfs.source_es_6_8.IndexMetadataFactory;
import com.rfs.source_es_6_8.ShardMetadata;
import com.rfs.source_es_6_8.ShardMetadataFactory;
import com.rfs.source_es_6_8.SnapshotMetadata;
import com.rfs.source_es_6_8.SnapshotMetadataFactory;
import com.rfs.source_es_6_8.SnapshotRepoData;
import com.rfs.source_es_6_8.SnapshotRepoDataProvider;
import com.rfs.source_es_6_8.SnapshotShardUnpacker;


public class DemoReindexDocuments {

    public static class Args {
        @Parameter(names = {"-s", "--snapshot-name"}, description = "The name of the snapshot to read", required = true)
        public String snapshotName;

        @Parameter(names = {"-d", "--snapshot-dir"}, description = "The absolute path to the snapshot directory", required = true)
        public String snapshotDirPath;

        @Parameter(names = {"-l", "--lucene-dir"}, description = "The absolute path to the directory where we'll put the Lucene docs", required = true)
        public String luceneBasePathString;

        @Parameter(names = {"-t", "--target-host"}, description = "The target host and port (e.g. http://localhost:9200)", required = true)
        public String targetHost;

        @Parameter(names = {"-u", "--target-username"}, description = "The target username", required = true)
        public String targetUser;

        @Parameter(names = {"-p", "--target-password"}, description = "The target password", required = true)
        public String targetPass;

        @Parameter(names = {"-v", "--source-version"}, description = "Source version", required = true, converter = SourceVersion.ArgsConverter.class)
        public SourceVersion sourceVersion;
    }


    public static void main(String[] args) {
        Args arguments = new Args();
        JCommander.newBuilder()
            .addObject(arguments)
            .build()
            .parse(args);
        
        String snapshotName = arguments.snapshotName;
        String snapshotDirPath = arguments.snapshotDirPath;
        String luceneBasePathString = arguments.luceneBasePathString;
        String targetHost = arguments.targetHost;
        String targetUser = arguments.targetUser;
        String targetPass = arguments.targetPass;
        SourceVersion sourceVersion = arguments.sourceVersion;
        Path luceneBasePath = Paths.get(luceneBasePathString);
        ConnectionDetails targetConnection = new ConnectionDetails(targetHost, targetUser, targetPass);

        if (sourceVersion != SourceVersion.ES_6_8) {
            System.out.println("Only ES_6_8 is supported");
            return;
        }

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
            SnapshotMetadata snapshotMetadata = SnapshotMetadataFactory.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);
            System.out.println("Snapshot data read successfully");

            // ==========================================================================================================
            // Read all the Index Metadata
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Attempting to read Index Metadata...");
            Map<String, IndexMetadata> indexMetadatas = new HashMap<>();
            for (SnapshotRepoData.Index index : repoDataProvider.getIndicesInSnapshot(snapshotName)) {
                System.out.println("Reading Index Metadata for index: " + index.name);
                IndexMetadata indexMetadata = IndexMetadataFactory.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName, index.name);
                indexMetadatas.put(index.name, indexMetadata);
            }
            System.out.println("Index Metadata read successfully");

            // ==========================================================================================================
            // Unpack the snapshot blobs
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Unpacking blob files to disk...");

            for (IndexMetadata indexMetadata : indexMetadatas.values()) {
                System.out.println("Processing index: " + indexMetadata.getName());
                for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
                    System.out.println("=== Shard ID: " + shardId + " ===");

                    // Get the shard metadata
                    ShardMetadata shardMetadata = ShardMetadataFactory.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName, indexMetadata.getName(), shardId);
                    SnapshotShardUnpacker.unpack(shardMetadata, Paths.get(snapshotDirPath), luceneBasePath);
                }
            }

            System.out.println("Blob files unpacked successfully");

            // ==========================================================================================================
            // Reindex the documents
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Reindexing the documents...");

            for (IndexMetadata indexMetadata : indexMetadatas.values()) {
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
