package com.rfs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.ComponentTemplateMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.elasticsearch.script.ScriptMetadata;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;

public class DemoPrintOutSnapshot {

    public DemoPrintOutSnapshot() {}

    public static void main(String[] args) {
        // Constants
        String snapshotName = "global_state_snapshot";
        String snapshotDirPath = "/Users/chelma/workspace/ElasticSearch/elasticsearch/build/testclusters/runTask-0/repo/snapshots";        
        String luceneFilesBasePath = "/tmp/lucene_files";

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
            SnapshotMetadataProvider snapshotMetadataProvider = SnapshotMetadataProvider.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);
            System.out.println("Snapshot State: " + snapshotMetadataProvider.getState());
            System.out.println("Snapshot State Reason: " + snapshotMetadataProvider.getReason());
            System.out.println("Snapshot Indices: " + snapshotMetadataProvider.getIndices());
            System.out.println("Snapshot Shards Total: " + snapshotMetadataProvider.getShardsTotal());
            System.out.println("Snapshot Shards Successful: " + snapshotMetadataProvider.getShardsSuccessful());
            System.out.println("Snapshot Shards Failed: " + snapshotMetadataProvider.getShardsFailed());

            // ==========================================================================================================
            // Read the Global Metadata
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Attempting to read Global Metadata details...");


            GlobalMetadataProvider globalMetadataProvider = GlobalMetadataProvider.fromSnapshotRepoDataProvider(repoDataProvider, snapshotName);

            List<String> componentKeys = new ArrayList<>();
            globalMetadataProvider.getComponentTemplates().fieldNames().forEachRemaining(componentKeys::add);
            System.out.println("Global Component Templates Keys: " + componentKeys);

            List<String> indexKeys = new ArrayList<>();
            globalMetadataProvider.getIndexTemplates().fieldNames().forEachRemaining(indexKeys::add);
            System.out.println("Global Index Templates Keys: " + indexKeys);

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

                System.out.println("Index Id: " + indexMetadataProvider.getId());
                System.out.println("Index Number of Shards: " + indexMetadataProvider.getNumberOfShards());
                System.out.println("Index Settings: " + indexMetadataProvider.getSettingsJson().toString());
                System.out.println("Index Mappings: " + indexMetadataProvider.getMappingsJson().toString());
                System.out.println("Index Aliases: " + indexMetadataProvider.getAliasesJson().toString());
            }
            System.out.println("Index Metadata read successfully");

            // ==========================================================================================================
            // Read the Index Shard Metadata for the Snapshot
            // ==========================================================================================================
            // System.out.println("==================================================================");
            // System.out.println("Attempting to read Index Shard Metadata...");

            // List<IndexMetadataProvider> filteredIndexMetadatas = new ArrayList<>();
            // repoDataProvider.getIndicesInSnapshot(snapshotName).forEach(index -> {
            //     filteredIndexMetadatas.add(indexMetadatas.get(index.name));
            // });

            // for (IndexMetadataProvider indexMetadata : filteredIndexMetadatas) {
            //     System.out.println("Reading Index Shard Metadata for index: " + indexMetadata.getName());
            //     for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
            //         System.out.println("=== Shard ID: " + shardId + " ===");

            //         // Get the file mapping for the shard
            //         String snapshotIndexShardDirPath = snapshotDirPath + "/indices/" + indexMetadata.getId() + "/" + shardId;
            //         BlobStoreIndexShardSnapshot shardSnapshot = readIndexShardMetadata(snapshotDirPath, snapshotIndexShardDirPath, snapshotId);
            //         SnapshotFiles snapshotFiles = new SnapshotFiles(shardSnapshot.snapshot(), shardSnapshot.indexFiles(), null);
            //         for (BlobStoreIndexShardSnapshot.FileInfo fileInfo : snapshotFiles.indexFiles()) {
            //             System.out.println("File Info: " + fileInfo.toString());
            //         }
            //     }
            // }

            // ==========================================================================================================
            // Unpack the blob files
            // ==========================================================================================================
            // System.out.println("==================================================================");
            // System.out.println("Unpacking blob files to disk...");

            // NativeFSLockFactory lockFactory = NativeFSLockFactory.INSTANCE;

            // for (SnapshotRepoData.Index index : repoDataProvider.getIndicesInSnapshot(snapshotName)) {
            //     String snapshotIndexDirPath = snapshotDirPath + "/indices/" + index.id;
                
            //     String indexMetadataId = repoDataProvider.getIndexMetadataId(snapshotName, index.name);

            //     IndexMetadata indexMetadata = readIndexMetadata(snapshotDirPath, snapshotIndexDirPath, indexMetadataId);
            //     for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
            //         System.out.println("=== Index: " + index.id + " Shard ID: " + shardId + " ===");

            //         // Get the file mapping for the shard
            //         String snapshotIndexShardDirPath = snapshotDirPath + "/indices/" + index.id + "/" + shardId;
            //         BlobStoreIndexShardSnapshot shardSnapshot = readIndexShardMetadata(snapshotDirPath, snapshotIndexShardDirPath, snapshotId);
            //         SnapshotFiles snapshotFiles = new SnapshotFiles(shardSnapshot.snapshot(), shardSnapshot.indexFiles(), null);

            //         // Create the blob container
            //         Path snapshotPath = Paths.get(snapshotDirPath);
            //         BlobPath blobPath = new BlobPath().add(snapshotIndexShardDirPath);
            
            //         FsBlobStore blobStore = new FsBlobStore(bufferSizeInBytes, snapshotPath, false);
            //         BlobContainer container = blobStore.blobContainer(blobPath);
                    
            //         // Create the directory for the shard's lucene files
            //         Path lucene_dir = Paths.get(luceneFilesBasePath + "/" + index.name + "/" + shardId);
            //         Files.createDirectories(lucene_dir);
            //         final FSDirectory primaryDirectory = FSDirectory.open(lucene_dir, lockFactory);
                    
            //         for (BlobStoreIndexShardSnapshot.FileInfo fileInfo : snapshotFiles.indexFiles()) {
            //             System.out.println("Unpacking - Blob Name: " + fileInfo.name() + ", Lucene Name: " + fileInfo.metadata().name());
            //             IndexOutput indexOutput = primaryDirectory.createOutput(fileInfo.metadata().name(), IOContext.DEFAULT);

            //             if (fileInfo.name().startsWith("v__")) {
            //                 final BytesRef hash = fileInfo.metadata().hash();
            //                 indexOutput.writeBytes(hash.bytes, hash.offset, hash.length);
            //             } else {
            //                 try (InputStream stream = new SlicedInputStream(fileInfo.numberOfParts()) {
            //                     @Override
            //                     protected InputStream openSlice(int slice) throws IOException {
            //                         return container.readBlob(fileInfo.partName(slice));
            //                     }
            //                 }) {
            //                     final byte[] buffer = new byte[Math.toIntExact(Math.min(bufferSizeInBytes, fileInfo.length()))];
            //                     int length;
            //                     while ((length = stream.read(buffer)) > 0) {
            //                         indexOutput.writeBytes(buffer, 0, length);
            //                     }
            //                 }
            //             }
            //             blobStore.close();
            //             indexOutput.close();
            //         }

            //         // Now, read the documents back out
            //         System.out.println("--- Reading docs in the shard ---");
            //         readDocumentsFromLuceneIndex(lucene_dir.toAbsolutePath().toString());                    
            //     }
            // }

            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static BlobStoreIndexShardSnapshot readIndexShardMetadata(String snapshotDirPath, String snapshotIndexShardDirPath, SnapshotId snapshotId) throws Exception {
        Path snapshotPath = Paths.get(snapshotDirPath);
        BlobPath blobPath = new BlobPath().add(snapshotIndexShardDirPath);

        FsBlobStore blobStore = new FsBlobStore(
            ElasticsearchConstants.BUFFER_SIZE_IN_BYTES, // Magic number pulled from running ES process
            snapshotPath, 
            false
        );
        BlobContainer container = blobStore.blobContainer(blobPath);

        ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshot> indexShardSnapshotFormat =
            new ChecksumBlobStoreFormat<>("snapshot", "snap-%s.dat", BlobStoreIndexShardSnapshot::fromXContent);

        BlobStoreIndexShardSnapshot shardSnapshot = indexShardSnapshotFormat.read(container, snapshotId.getUUID(), ElasticsearchConstants.EMPTY_REGISTRY);

        blobStore.close();

        return shardSnapshot;
    }

    private static void readDocumentsFromLuceneIndex(String indexDirectoryPath) throws Exception {
        // Opening the directory that contains the Lucene index
        try (FSDirectory directory = FSDirectory.open(Paths.get(indexDirectoryPath));
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
                List<org.apache.lucene.index.IndexableField> fields = document.getFields();
                for (org.apache.lucene.index.IndexableField field : fields) {
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