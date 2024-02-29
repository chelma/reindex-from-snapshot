package com.rfs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.util.BytesRef;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;

public class PrintOutDemo {

    public PrintOutDemo() {}

    public static void main(String[] args) {
        // Constants
        String repoDataFileName = "index-8"; // Changes when the snapshot repo changes, unsafe to hardcode
        int bufferSizeInBytes = 131072; // Magic number pulled from running ES process

        // Paths
        String snapshotName = "global_state_snapshot";
        String snapshotDirPath = "/Users/chelma/workspace/ElasticSearch/elasticsearch/build/testclusters/runTask-0/repo/snapshots";        
        String luceneFilesBasePath = "/tmp/lucene_files";

        try {
            // ==========================================================================================================
            // Read the Repo data file
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Attempting to read Repo data file...");
            String repoDataFilePath = snapshotDirPath + "/" + repoDataFileName;
            SnapshotRepoDataProvider repoDataProvider = new SnapshotRepoDataProvider(repoDataFilePath);
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
            SnapshotId snapshotId = new SnapshotId(snapshotName, snapshotIdString);
            SnapshotInfo snapshotInfo = readSnapshotDetails(snapshotDirPath, snapshotId);
            System.out.println("Snapshot details: " + snapshotInfo.toString());

            // ==========================================================================================================
            // Read all the Index Metadata
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Attempting to read Index Metadata...");
            for (SnapshotRepoData.Index index : repoDataProvider.getIndicesInSnapshot(snapshotName)) {
                System.out.println("Reading Index Metadata for index: " + index.name);
                String snapshotIndexDirPath = snapshotDirPath + "/indices/" + index.id;
                
                String indexMetadataId = repoDataProvider.getIndexMetadataId(snapshotName, index.name);

                IndexMetadata indexMetadata = readIndexMetadata(snapshotDirPath, snapshotIndexDirPath, indexMetadataId);
                System.out.println("Index Id: " + indexMetadata.getIndex().getUUID());
                System.out.println("Index Number of Shards: " + indexMetadata.getNumberOfShards());
                System.out.println("Index Settings: " + indexMetadata.getSettings().toDelimitedString(','));
                System.out.println("Index Mappings: " + indexMetadata.mapping().source().toString());
                System.out.println("Index Aliases: " + indexMetadata.getAliases().toString());
                System.out.println(indexMetadata.getAliases().keys());
                System.out.println(indexMetadata.getAliases().values());

                ConnectionDetails connectionDetails = new ConnectionDetails("localhost", 9200, "elastic-admin", "elastic-password");
                IndexCreator.createIndex(index.name + "_reindexed", indexMetadata, connectionDetails);
            }

            // ==========================================================================================================
            // Read all the Index Shard Metadata
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Attempting to read Index Shard Metadata...");

            for (SnapshotRepoData.Index index : repoDataProvider.getIndicesInSnapshot(snapshotName)) {
                System.out.println("Reading Index Shard Metadata for index: " + index.name);
                String snapshotIndexDirPath = snapshotDirPath + "/indices/" + index.id;
                
                String indexMetadataId = repoDataProvider.getIndexMetadataId(snapshotName, index.name);

                IndexMetadata indexMetadata = readIndexMetadata(snapshotDirPath, snapshotIndexDirPath, indexMetadataId);
                for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
                    System.out.println("Shard ID: " + shardId);
                    String snapshotIndexShardDirPath = snapshotDirPath + "/indices/" + index.id + "/" + shardId;
                    BlobStoreIndexShardSnapshot shardSnapshot = readIndexShardMetadata(snapshotDirPath, snapshotIndexShardDirPath, snapshotId);
                    SnapshotFiles snapshotFiles = new SnapshotFiles(shardSnapshot.snapshot(), shardSnapshot.indexFiles(), null);
                    for (BlobStoreIndexShardSnapshot.FileInfo fileInfo : snapshotFiles.indexFiles()) {
                        System.out.println("File Info: " + fileInfo.toString());
                    }
                }
            }

            // ==========================================================================================================
            // Unpack the blob files
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Unpacking blob files to disk...");

            NativeFSLockFactory lockFactory = NativeFSLockFactory.INSTANCE;

            for (SnapshotRepoData.Index index : repoDataProvider.getIndicesInSnapshot(snapshotName)) {
                String snapshotIndexDirPath = snapshotDirPath + "/indices/" + index.id;
                
                String indexMetadataId = repoDataProvider.getIndexMetadataId(snapshotName, index.name);

                IndexMetadata indexMetadata = readIndexMetadata(snapshotDirPath, snapshotIndexDirPath, indexMetadataId);
                for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
                    System.out.println("=== Index: " + index.id + " Shard ID: " + shardId + " ===");

                    // Get the file mapping for the shard
                    String snapshotIndexShardDirPath = snapshotDirPath + "/indices/" + index.id + "/" + shardId;
                    BlobStoreIndexShardSnapshot shardSnapshot = readIndexShardMetadata(snapshotDirPath, snapshotIndexShardDirPath, snapshotId);
                    SnapshotFiles snapshotFiles = new SnapshotFiles(shardSnapshot.snapshot(), shardSnapshot.indexFiles(), null);

                    // Create the blob container
                    Path snapshotPath = Paths.get(snapshotDirPath);
                    BlobPath blobPath = new BlobPath().add(snapshotIndexShardDirPath);
            
                    FsBlobStore blobStore = new FsBlobStore(bufferSizeInBytes, snapshotPath, false);
                    BlobContainer container = blobStore.blobContainer(blobPath);
                    
                    // Create the directory for the shard's lucene files
                    Path lucene_dir = Paths.get(luceneFilesBasePath + "/" + index.name + "/" + shardId);
                    Files.createDirectories(lucene_dir);
                    final FSDirectory primaryDirectory = FSDirectory.open(lucene_dir, lockFactory);
                    
                    for (BlobStoreIndexShardSnapshot.FileInfo fileInfo : snapshotFiles.indexFiles()) {
                        System.out.println("Unpacking - Blob Name: " + fileInfo.name() + ", Lucene Name: " + fileInfo.metadata().name());
                        IndexOutput indexOutput = primaryDirectory.createOutput(fileInfo.metadata().name(), IOContext.DEFAULT);

                        if (fileInfo.name().startsWith("v__")) {
                            final BytesRef hash = fileInfo.metadata().hash();
                            indexOutput.writeBytes(hash.bytes, hash.offset, hash.length);
                        } else {
                            try (InputStream stream = new SlicedInputStream(fileInfo.numberOfParts()) {
                                @Override
                                protected InputStream openSlice(int slice) throws IOException {
                                    return container.readBlob(fileInfo.partName(slice));
                                }
                            }) {
                                final byte[] buffer = new byte[Math.toIntExact(Math.min(bufferSizeInBytes, fileInfo.length()))];
                                int length;
                                while ((length = stream.read(buffer)) > 0) {
                                    indexOutput.writeBytes(buffer, 0, length);
                                }
                            }
                        }
                        blobStore.close();
                        indexOutput.close();
                    }

                    // Now, read the documents back out
                    System.out.println("--- Reading docs in the shard ---");
                    readDocumentsFromLuceneIndex(lucene_dir.toAbsolutePath().toString());                    
                }
            }

            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static SnapshotInfo readSnapshotDetails(String snapshotDirPath, SnapshotId snapshotId) throws Exception {
        Path snapshotPath = Paths.get(snapshotDirPath);
        BlobPath blobPath = new BlobPath();
        blobPath.add(snapshotDirPath);

        FsBlobStore blobStore = new FsBlobStore(
            131072, // Magic number pulled from running ES process
            snapshotPath, 
            false
        );
        BlobContainer container = blobStore.blobContainer(blobPath);

        ChecksumBlobStoreFormat<SnapshotInfo> snapshotFormat =
            new ChecksumBlobStoreFormat<>("snapshot", "snap-%s.dat", SnapshotInfo::fromXContentInternal);

        // Make an empty registry; may not work for all cases?
        List<NamedXContentRegistry.Entry> entries = List.of();
        NamedXContentRegistry registry = new NamedXContentRegistry(entries);

        // Read the snapshot details
        SnapshotInfo snapshotInfo = snapshotFormat.read(container, snapshotId.getUUID(), registry);

        blobStore.close();

        return snapshotInfo;
    }

    private static IndexMetadata readIndexMetadata(String snapshotDirPath, String snapshotIndexDirPath, String indexMetadataId) throws Exception {
        Path snapshotPath = Paths.get(snapshotDirPath);
        BlobPath blobPath = new BlobPath().add(snapshotIndexDirPath);

        FsBlobStore blobStore = new FsBlobStore(
            131072, // Magic number pulled from running ES process
            snapshotPath, 
            false
        );
        BlobContainer container = blobStore.blobContainer(blobPath);

        ChecksumBlobStoreFormat<IndexMetadata> indexMetadataFormat =
            new ChecksumBlobStoreFormat<>("index-metadata", "meta-%s.dat", IndexMetadata::fromXContent);

        // Make an empty registry; may not work for all cases?
        List<NamedXContentRegistry.Entry> entries = List.of();
        NamedXContentRegistry registry = new NamedXContentRegistry(entries);
        
        IndexMetadata indexMetadata = indexMetadataFormat.read(container, indexMetadataId, registry);

        blobStore.close();

        return indexMetadata;
    }

    private static BlobStoreIndexShardSnapshot readIndexShardMetadata(String snapshotDirPath, String snapshotIndexShardDirPath, SnapshotId snapshotId) throws Exception {
        Path snapshotPath = Paths.get(snapshotDirPath);
        BlobPath blobPath = new BlobPath().add(snapshotIndexShardDirPath);

        FsBlobStore blobStore = new FsBlobStore(
            131072, // Magic number pulled from running ES process
            snapshotPath, 
            false
        );
        BlobContainer container = blobStore.blobContainer(blobPath);

        ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshot> indexShardSnapshotFormat =
            new ChecksumBlobStoreFormat<>("snapshot", "snap-%s.dat", BlobStoreIndexShardSnapshot::fromXContent);

        // Make an empty registry; may not work for all cases?
        List<NamedXContentRegistry.Entry> entries = List.of();
        NamedXContentRegistry registry = new NamedXContentRegistry(entries);

        BlobStoreIndexShardSnapshot shardSnapshot = indexShardSnapshotFormat.read(container, snapshotId.getUUID(), registry);

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