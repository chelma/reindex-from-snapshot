package com.rfs;

import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import com.rfs.common.Uid;
import com.rfs.common.SourceVersion;
import com.rfs.source_es_6_8.GlobalMetadata;
import com.rfs.source_es_6_8.GlobalMetadataFactory;
import com.rfs.source_es_6_8.IndexMetadata;
import com.rfs.source_es_6_8.IndexMetadataFactory;
import com.rfs.source_es_6_8.ShardMetadata;
import com.rfs.source_es_6_8.ShardMetadataFactory;
import com.rfs.source_es_6_8.SnapshotMetadata;
import com.rfs.source_es_6_8.SnapshotMetadataFactory;
import com.rfs.source_es_6_8.SnapshotRepoData;
import com.rfs.source_es_6_8.SnapshotRepoDataProvider;
import com.rfs.source_es_6_8.SnapshotShardUnpacker;

public class DemoPrintOutSnapshot {

    public static class Args {
        @Parameter(names = {"-s", "--snapshot-name"}, description = "The name of the snapshot to read", required = true)
        public String snapshotName;

        @Parameter(names = {"-d", "--snapshot-dir"}, description = "The absolute path to the snapshot directory", required = true)
        public String snapshotDirPath;

        @Parameter(names = {"-l", "--lucene-dir"}, description = "The absolute path to the directory where we'll put the Lucene docs", required = true)
        public String luceneBasePathString;

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
        SourceVersion sourceVersion = arguments.sourceVersion;

        if (sourceVersion != SourceVersion.ES_6_8) {
            System.out.println("Only ES_6_8 is supported");
            return;
        }

        try {
            // ==========================================================================================================
            // Read the Repo data file
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Attempting to read Repo data file...");
            SnapshotRepoDataProvider repoDataProvider = new SnapshotRepoDataProvider(Paths.get(snapshotDirPath));
            System.out.println("Snapshots: ");
            repoDataProvider.getSnapshots().forEach(snapshot -> System.out.println(snapshot.name + " - " + snapshot.uuid));

            System.out.println("Indices: ");
            repoDataProvider.getIndices().forEach(index -> System.out.println(index.name + " - " + index.id));
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
            SnapshotMetadata snapshotMetadata = SnapshotMetadataFactory.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);
            System.out.println("Snapshot Metadata State: " + snapshotMetadata.getState());
            System.out.println("Snapshot Metadata State Reason: " + snapshotMetadata.getReason());
            System.out.println("Snapshot Metadata Version: " + snapshotMetadata.getVersionId());
            System.out.println("Snapshot Metadata Indices: " + snapshotMetadata.getIndices());
            System.out.println("Snapshot Metadata Shards Total: " + snapshotMetadata.getTotalShards());
            System.out.println("Snapshot Metadata Shards Successful: " + snapshotMetadata.getSuccessfulShards());

            // ==========================================================================================================
            // Read the Global Metadata
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Attempting to read Global Metadata details...");

            GlobalMetadata globalMetadata = GlobalMetadataFactory.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);

            List<String> templateKeys = new ArrayList<>();
            globalMetadata.getTemplates().fieldNames().forEachRemaining(templateKeys::add);
            System.out.println("Global Templates Keys: " + templateKeys);

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

                System.out.println("Index Id: " + indexMetadata.getId());
                System.out.println("Index Number of Shards: " + indexMetadata.getNumberOfShards());
                System.out.println("Index Settings: " + indexMetadata.getSettings().toString());
                System.out.println("Index Mappings: " + indexMetadata.getMappings().toString());
                System.out.println("Index Aliases: " + indexMetadata.getAliases().toString());
            }
            System.out.println("Index Metadata read successfully");

            // ==========================================================================================================
            // Read the Index Shard Metadata for the Snapshot
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Attempting to read Index Shard Metadata...");
            for (IndexMetadata indexMetadata : indexMetadatas.values()) {
                System.out.println("Reading Index Shard Metadata for index: " + indexMetadata.getName());
                for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
                    System.out.println("=== Shard ID: " + shardId + " ===");

                    // Get the file mapping for the shard
                    ShardMetadata shardMetadata = ShardMetadataFactory.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName, indexMetadata.getName(), shardId);
                    System.out.println("Shard Metadata: " + shardMetadata.toString());
                }
            }

            // ==========================================================================================================
            // Unpack the blob files
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Unpacking blob files to disk...");

            for (IndexMetadata indexMetadata : indexMetadatas.values()){
                for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
                    ShardMetadata shardMetadata = ShardMetadataFactory.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName, indexMetadata.getName(), shardId);
                    SnapshotShardUnpacker.unpack(shardMetadata, Paths.get(snapshotDirPath), Paths.get(luceneBasePathString));

                    // Now, read the documents back out
                    System.out.println("--- Reading docs in the shard ---");
                    Path luceneIndexDir = Paths.get(luceneBasePathString + "/" + shardMetadata.getIndexName() + "/" + shardMetadata.getShardId());
                    readDocumentsFromLuceneIndex(luceneIndexDir);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void readDocumentsFromLuceneIndex(Path indexDirectoryPath) throws Exception {
        // Opening the directory that contains the Lucene index
        try (FSDirectory directory = FSDirectory.open(indexDirectoryPath);
             IndexReader reader = DirectoryReader.open(directory)) {

            // Iterating over all documents in the index
            for (int i = 0; i < reader.maxDoc(); i++) {
                System.out.println("Reading Document");
                Document document = reader.document(i);

                BytesRef source_bytes = document.getBinaryValue("_source");
                if (source_bytes == null || source_bytes.bytes.length == 0) { // Skip deleted documents
                    String id = Uid.decodeId(reader.document(i).getBinaryValue("_id").bytes);
                    System.out.println("Document " + id + " is deleted");
                    continue;
                }              

                // Iterate over all fields in the document
                List<IndexableField> fields = document.getFields();
                for (IndexableField field : fields) {
                    if ("_source".equals(field.name())){
                        String source_string = source_bytes.utf8ToString();
                        System.out.println("Field name: " + field.name() + ", Field value: " + source_string);
                    } else if ("_id".equals(field.name())){
                        String uid = Uid.decodeId(document.getBinaryValue(field.name()).bytes);
                        System.out.println("Field name: " + field.name() + ", Field value: " + uid);
                    } else {
                        System.out.println("Field name: " + field.name());
                    }
                }
            }
        }
    }
}