package com.rfs;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import com.rfs.common.ConnectionDetails;
import com.rfs.common.IndexCreator;
import com.rfs.common.SourceVersion;
import com.rfs.source_es_6_8.GlobalMetadata;
import com.rfs.source_es_6_8.GlobalMetadataCreator;
import com.rfs.source_es_6_8.GlobalMetadataFactory;
import com.rfs.source_es_6_8.IndexMetadata;
import com.rfs.source_es_6_8.IndexMetadataFactory;
import com.rfs.source_es_6_8.SnapshotMetadata;
import com.rfs.source_es_6_8.SnapshotMetadataFactory;
import com.rfs.source_es_6_8.SnapshotRepoData;
import com.rfs.source_es_6_8.SnapshotRepoDataProvider;
import com.rfs.source_es_6_8.Transformer_to_OS_2_11;

public class DemoRecreateIndices {
    public static class Args {
        @Parameter(names = {"-s", "--snapshot-name"}, description = "The name of the snapshot to read", required = true)
        public String snapshotName;

        @Parameter(names = {"-d", "--snapshot-dir"}, description = "The absolute path to the snapshot directory", required = true)
        public String snapshotDirPath;

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
        String targetHost = arguments.targetHost;
        String targetUser = arguments.targetUser;
        String targetPass = arguments.targetPass;
        SourceVersion sourceVersion = arguments.sourceVersion;
        ConnectionDetails targetConnection = new ConnectionDetails(targetHost, targetUser, targetPass);

        if (sourceVersion != SourceVersion.ES_6_8) {
            System.out.println("Only ES_6_8 is supported");
            return;
        }

        // Should probably be passed in as an argument
        String[] templateWhitelist = {"posts_index_template"};

        // Should determine the source, target versions programmatically.  The dimensionality is from the target I'm using
        // to test.
        Transformer_to_OS_2_11 transformer = new Transformer_to_OS_2_11(3);

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
            SnapshotMetadata snapshotMetadata = SnapshotMetadataFactory.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);
            System.out.println("Snapshot data read successfully");

            // ==========================================================================================================
            // Read the Global Metadata
            // ==========================================================================================================
            System.out.println("Attempting to read Global Metadata details...");
            GlobalMetadata globalMetadata = GlobalMetadataFactory.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);
            System.out.println("Global Metadata read successfully");

            // ==========================================================================================================
            // Recreate the Global Metadata
            // ==========================================================================================================
            System.out.println("Attempting to recreate the Global Metadata...");
            GlobalMetadataCreator.create(globalMetadata, targetConnection, templateWhitelist, transformer);

            // ==========================================================================================================
            // Read all the Index Metadata
            // ==========================================================================================================
            System.out.println("Attempting to read Index Metadata...");
            List<IndexMetadata> indexMetadatas = new ArrayList<>();
            for (SnapshotRepoData.Index index : repoDataProvider.getIndicesInSnapshot(snapshotName)) {
                System.out.println("Reading Index Metadata for index: " + index.name);
                IndexMetadata indexMetadata = IndexMetadataFactory.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName, index.name);
                indexMetadatas.add(indexMetadata);
            }
            System.out.println("Index Metadata read successfully");

            // ==========================================================================================================
            // Recreate the indices
            // ==========================================================================================================
            System.out.println("Attempting to recreate the indices...");
            for (IndexMetadata indexMetadata : indexMetadatas) {
                String reindexName = indexMetadata.getName() + "_reindexed";
                System.out.println("Recreating index " + indexMetadata.getName() + " as " + reindexName + " on target...");
                IndexCreator.create(reindexName, indexMetadata, targetConnection, transformer);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
