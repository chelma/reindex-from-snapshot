package com.rfs;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.rfs.common.*;
import com.rfs.version_es_6_8.*;
import com.rfs.version_es_7_10.*;


public class DemoReindexDocuments {

    public static class Args {
        @Parameter(names = {"-n", "--snapshot-name"}, description = "The name of the snapshot to read", required = true)
        public String snapshotName;

        @Parameter(names = {"-d", "--snapshot-dir"}, description = "The absolute path to the snapshot directory", required = true)
        public String snapshotDirPath;

        @Parameter(names = {"-l", "--lucene-dir"}, description = "The absolute path to the directory where we'll put the Lucene docs", required = true)
        public String luceneBasePathString;

        @Parameter(names = {"-h", "--target-host"}, description = "The target host and port (e.g. http://localhost:9200)", required = true)
        public String targetHost;

        @Parameter(names = {"-u", "--target-username"}, description = "The target username", required = true)
        public String targetUser;

        @Parameter(names = {"-p", "--target-password"}, description = "The target password", required = true)
        public String targetPass;

        @Parameter(names = {"-s", "--source-version"}, description = "Source version", required = true, converter = ClusterVersion.ArgsConverter.class)
        public ClusterVersion sourceVersion;

        @Parameter(names = {"-t", "--target-version"}, description = "Target version", required = true, converter = ClusterVersion.ArgsConverter.class)
        public ClusterVersion targetVersion;
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
        ClusterVersion sourceVersion = arguments.sourceVersion;
        ClusterVersion targetVersion = arguments.targetVersion;
        Path luceneBasePath = Paths.get(luceneBasePathString);
        ConnectionDetails targetConnection = new ConnectionDetails(targetHost, targetUser, targetPass);

        if (!((sourceVersion == ClusterVersion.ES_6_8) || (sourceVersion == ClusterVersion.ES_7_10))) {
            throw new IllegalArgumentException("Unsupported source version: " + sourceVersion);
        }

        if (targetVersion != ClusterVersion.OS_2_11) {
            throw new IllegalArgumentException("Unsupported target version: " + sourceVersion);
        }

        try {
            // ==========================================================================================================
            // Read the Repo data file
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Attempting to read Repo data file...");
            SnapshotRepo.Provider repoDataProvider;
            if (sourceVersion == ClusterVersion.ES_6_8) {
                repoDataProvider = new SnapshotRepoProvider_ES_6_8(Paths.get(snapshotDirPath));
            } else {
                repoDataProvider = new SnapshotRepoProvider_ES_7_10(Paths.get(snapshotDirPath));
            }
            System.out.println("Repo data read successfully");

            // ==========================================================================================================
            // Read the Snapshot details
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Attempting to read Snapshot details...");
            String snapshotIdString = repoDataProvider.getSnapshotId(snapshotName);

            if (snapshotIdString == null) {
                System.out.println("Snapshot not found");
                return;
            }
            SnapshotMetadata.Data snapshotMetadata;
            if (sourceVersion == ClusterVersion.ES_6_8) {
                snapshotMetadata = new SnapshotMetadataFactory_ES_6_8().fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);
            } else {
                snapshotMetadata = new SnapshotMetadataFactory_ES_7_10().fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);
            }
            System.out.println("Snapshot data read successfully");

            // ==========================================================================================================
            // Read all the Index Metadata
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Attempting to read Index Metadata...");
            List<IndexMetadata.Data> indexMetadatas = new ArrayList<>();
            for (SnapshotRepo.Index index : repoDataProvider.getIndicesInSnapshot(snapshotName)) {
                System.out.println("Reading Index Metadata for index: " + index.getName());
                IndexMetadata.Data indexMetadata;
                if (sourceVersion == ClusterVersion.ES_6_8) {
                    indexMetadata = new IndexMetadataFactory_ES_6_8().fromSnapshotRepoDataProvider(repoDataProvider, snapshotName, index.getName());
                } else {
                    indexMetadata = new IndexMetadataFactory_ES_7_10().fromSnapshotRepoDataProvider(repoDataProvider, snapshotName, index.getName());
                }
                indexMetadatas.add(indexMetadata);
            }
            System.out.println("Index Metadata read successfully");

            // ==========================================================================================================
            // Unpack the snapshot blobs
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Unpacking blob files to disk...");

            for (IndexMetadata.Data indexMetadata : indexMetadatas) {
                System.out.println("Processing index: " + indexMetadata.getName());
                for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
                    System.out.println("=== Shard ID: " + shardId + " ===");

                    // Get the shard metadata
                    ShardMetadata.Data shardMetadata;
                    if (sourceVersion == ClusterVersion.ES_6_8) {
                        shardMetadata = new ShardMetadataFactory_ES_6_8().fromSnapshotRepoDataProvider(repoDataProvider, snapshotName, indexMetadata.getName(), shardId);
                    } else {
                        shardMetadata = new ShardMetadataFactory_ES_7_10().fromSnapshotRepoDataProvider(repoDataProvider, snapshotName, indexMetadata.getName(), shardId);
                    }
                    SnapshotShardUnpacker.unpack(shardMetadata, Paths.get(snapshotDirPath), luceneBasePath);
                }
            }

            System.out.println("Blob files unpacked successfully");

            // ==========================================================================================================
            // Reindex the documents
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Reindexing the documents...");

            for (IndexMetadata.Data indexMetadata : indexMetadatas) {
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
