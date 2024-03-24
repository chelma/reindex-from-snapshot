package com.rfs;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rfs.common.ConnectionDetails;
import com.rfs.common.GlobalMetadata;
import com.rfs.common.IndexMetadata;
import com.rfs.common.SnapshotMetadata;
import com.rfs.common.SnapshotRepo;
import com.rfs.transformers.Transformer;
import com.rfs.transformers.Transformer_ES_6_8_to_OS_2_11;
import com.rfs.transformers.Transformer_ES_7_10_OS_2_11;
import com.rfs.common.ClusterVersion;
import com.rfs.version_es_6_8.*;
import com.rfs.version_es_7_10.*;
import com.rfs.version_os_2_11.*;

public class DemoRecreateIndices {
    public static class Args {
        @Parameter(names = {"-n", "--snapshot-name"}, description = "The name of the snapshot to read", required = true)
        public String snapshotName;

        @Parameter(names = {"-d", "--snapshot-dir"}, description = "The absolute path to the snapshot directory", required = true)
        public String snapshotDirPath;

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
        String targetHost = arguments.targetHost;
        String targetUser = arguments.targetUser;
        String targetPass = arguments.targetPass;
        ClusterVersion sourceVersion = arguments.sourceVersion;
        ClusterVersion targetVersion = arguments.targetVersion;
        ConnectionDetails targetConnection = new ConnectionDetails(targetHost, targetUser, targetPass);

        if (!((sourceVersion == ClusterVersion.ES_6_8) || (sourceVersion == ClusterVersion.ES_7_10))) {
            throw new IllegalArgumentException("Unsupported source version: " + sourceVersion);
        }

        if (targetVersion != ClusterVersion.OS_2_11) {
            throw new IllegalArgumentException("Unsupported target version: " + sourceVersion);
        }

        // Should probably be passed in as an arguments
        String[] templateWhitelist = {"posts_index_template"};
        String[] componentTemplateWhitelist = {"posts_template"};

        try {
            // ==========================================================================================================
            // Read the Repo data file
            // ==========================================================================================================
            System.out.println("Attempting to read Repo data file...");
            SnapshotRepo.Provider repoDataProvider;
            if (sourceVersion == ClusterVersion.ES_6_8) {
                repoDataProvider = new SnapshotRepoProvider_ES_6_8(Paths.get(snapshotDirPath));
            } else {
                repoDataProvider = new SnapshotRepoProvider_ES_7_10(Paths.get(snapshotDirPath));
            }
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
            SnapshotMetadata.Data snapshotMetadata;
            if (sourceVersion == ClusterVersion.ES_6_8) {
                snapshotMetadata = new SnapshotMetadataFactory_ES_6_8().fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);
            } else {
                snapshotMetadata = new SnapshotMetadataFactory_ES_7_10().fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);
            }
            System.out.println("Snapshot data read successfully");

            // ==========================================================================================================
            // Read the Global Metadata
            // ==========================================================================================================
            System.out.println("Attempting to read Global Metadata details...");
            GlobalMetadata.Data globalMetadata;
            if (sourceVersion == ClusterVersion.ES_6_8) {
                globalMetadata = new GlobalMetadataFactory_ES_6_8().fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);
            } else {
                globalMetadata = new GlobalMetadataFactory_ES_7_10().fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);
            }
            System.out.println("Global Metadata read successfully");

            // ==========================================================================================================
            // Recreate the Global Metadata
            // ==========================================================================================================
            System.out.println("Attempting to recreate the Global Metadata...");

            if (sourceVersion == ClusterVersion.ES_6_8) {
                if (targetVersion == ClusterVersion.OS_2_11) {
                    Transformer transformer = new Transformer_ES_6_8_to_OS_2_11(3);
                    ObjectNode root = globalMetadata.toObjectNode();
                    ObjectNode transformedRoot = transformer.transformGlobalMetadata(root);                    
                    GlobalMetadataCreator_OS_2_11.create(transformedRoot, targetConnection, new String[0], templateWhitelist);
                } else {
                    throw new IllegalArgumentException("Unsupported target version " + targetVersion + " for source version " + sourceVersion);
                }                
            } else if (sourceVersion == ClusterVersion.ES_7_10) {
                if (targetVersion == ClusterVersion.OS_2_11) {
                    Transformer transformer = new Transformer_ES_7_10_OS_2_11(3);
                    ObjectNode root = globalMetadata.toObjectNode();
                    ObjectNode transformedRoot = transformer.transformGlobalMetadata(root);                    
                    GlobalMetadataCreator_OS_2_11.create(transformedRoot, targetConnection, componentTemplateWhitelist, templateWhitelist);
                } else {
                    throw new IllegalArgumentException("Unsupported target version " + targetVersion + " for source version " + sourceVersion);
                }
            }

            // ==========================================================================================================
            // Read all the Index Metadata
            // ==========================================================================================================
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
            // Recreate the indices
            // ==========================================================================================================
            System.out.println("Attempting to recreate the indices...");
            for (IndexMetadata.Data indexMetadata : indexMetadatas) {
                String reindexName = indexMetadata.getName() + "_reindexed";
                System.out.println("Recreating index " + indexMetadata.getName() + " as " + reindexName + " on target...");

                if (sourceVersion == ClusterVersion.ES_6_8) {
                    if (targetVersion == ClusterVersion.OS_2_11) {
                        Transformer transformer = new Transformer_ES_6_8_to_OS_2_11(3);
                        ObjectNode root = indexMetadata.toObjectNode();
                        ObjectNode transformedRoot = transformer.transformIndexMetadata(root);
                        IndexMetadataData_OS_2_11 indexMetadataOS211 = new IndexMetadataData_OS_2_11(transformedRoot, indexMetadata.getId(), reindexName);
                        IndexCreator_OS_2_11.create(reindexName, indexMetadataOS211, targetConnection);
                    } else {
                        throw new IllegalArgumentException("Unsupported target version " + targetVersion + " for source version " + sourceVersion);
                    }                
                } else if (sourceVersion == ClusterVersion.ES_7_10) {
                    if (targetVersion == ClusterVersion.OS_2_11) {
                        Transformer transformer = new Transformer_ES_7_10_OS_2_11(3);
                        ObjectNode root = indexMetadata.toObjectNode();
                        ObjectNode transformedRoot = transformer.transformIndexMetadata(root);
                        IndexMetadataData_OS_2_11 indexMetadataOS211 = new IndexMetadataData_OS_2_11(transformedRoot, indexMetadata.getId(), reindexName);
                        IndexCreator_OS_2_11.create(reindexName, indexMetadataOS211, targetConnection);
                    } else {
                        throw new IllegalArgumentException("Unsupported target version " + targetVersion + " for source version " + sourceVersion);
                    }
                }



                // IndexCreator.create(reindexName, indexMetadata, targetConnection, transformer);
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
