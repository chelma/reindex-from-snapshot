package com.rfs;

import java.io.File;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;

import com.rfs.RepoData;

public class LuceneDocumentReader {

    public LuceneDocumentReader() {
        // Add any initialization code here
    }

    public static void main(String[] args) {
        // Paths
        String snapshotName = "my_snapshot";
        String snapshotDirPath = "/Users/chelma/workspace/ElasticSearch/elasticsearch/build/testclusters/runTask-0/repo/snapshots";
        String repoDataFileName = "index-2";
        // String luceneIndexDirectoryPath = "/Users/chelma/workspace/ElasticSearch/elasticsearch/build/testclusters/runTask-0/data/nodes/0/indices/yflN6Nk6TVioQccGgHU1UQ/0/index";

        try {
            // ==========================================================================================================
            // Read the Repo data file
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Attempting to read Repo data file...");
            RepoData data = readRepoData(snapshotDirPath, repoDataFileName);
            System.out.println("Snapshots: ");
            data.snapshots.forEach(snapshot -> System.out.println(snapshot.name + " - " + snapshot.uuid));

            System.out.println("Indices: ");
            data.indices.forEach((name, index) -> System.out.println(name + " - " + index.id));
            System.out.println("Repo data read successfully");

            // ==========================================================================================================
            // Read the Snapshot details
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Attempting to read Snapshot details...");
            Optional<RepoData.Snapshot> mySnapshotOption = data.snapshots.stream()
                    .filter(snapshot -> snapshotName.equals(snapshot.name))
                    .findFirst();

            if (mySnapshotOption.isEmpty()) {
                System.out.println("Snapshot not found");
                return;
            }
            RepoData.Snapshot mySnapshot = mySnapshotOption.get();
            SnapshotId snapshotId = new SnapshotId(mySnapshot.name, mySnapshot.uuid);
            SnapshotInfo snapshotInfo = readSnapshotDetails(snapshotDirPath, snapshotId);
            System.out.println("Snapshot details: " + snapshotInfo.toString());

            // ==========================================================================================================
            // Read the all the Index Metadata
            // ==========================================================================================================
            System.out.println("==================================================================");
            System.out.println("Attempting to read Index Metadata...");
            for (String indexName : data.indices.keySet()) {
                System.out.println("Reading Index Metadata for index: " + indexName);
                RepoData.Index index = data.indices.get(indexName);
                IndexId indexId = new IndexId(indexName, index.id);
                String snapshotIndexDirPath = snapshotDirPath + "/indices/" + index.id;
                
                String indexMetadataKey = mySnapshot.indexMetadataLookup.get(index.id);
                String indexMetadataId = data.indexMetadataIdentifiers.get(indexMetadataKey);

                IndexMetadata indexMetadata = readIndexMetadata(snapshotDirPath, snapshotIndexDirPath, indexMetadataId);
                System.out.println("Index Id: " + indexMetadata.getIndex().getUUID());
                System.out.println("Index Number of Shards: " + indexMetadata.getNumberOfShards());
                System.out.println("Index Settings: " + indexMetadata.getSettings().toString());
                System.out.println("Index Mappings: " + indexMetadata.mapping().source().toString());
            }

            // System.out.println("Starting document reads...");
            // readDocumentsFromLuceneIndex(luceneIndexDirectoryPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static RepoData readRepoData(String snapshotDirPath, String repoDataName) throws Exception {
        String repoDataFilePath = snapshotDirPath + "/" + repoDataName;
        ObjectMapper mapper = new ObjectMapper();
        RepoData data = mapper.readValue(new File(repoDataFilePath), RepoData.class);
        return data;
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

    // private static void readDocumentsFromLuceneIndex(String indexDirectoryPath) throws Exception {
    //     // Opening the directory that contains the Lucene index
    //     try (FSDirectory directory = FSDirectory.open(Paths.get(indexDirectoryPath));
    //          IndexReader reader = DirectoryReader.open(directory)) {

    //         // Iterating over all documents in the index
    //         for (int i = 0; i < reader.maxDoc(); i++) {
    //             System.out.println("Reading Document");
    //             Document document = reader.document(i);

    //             BytesRef source_bytes = document.getBinaryValue("_source");
    //             if (source_bytes == null || source_bytes.bytes.length == 0) { // Skip deleted documents
    //                 String id = Uid.decodeId(reader.document(i).getBinaryValue("_id").bytes);
    //                 System.out.println("Document " + id + " is deleted");
    //                 continue;
    //             }              

    //             // Iterate over all fields in the document
    //             List<org.apache.lucene.index.IndexableField> fields = document.getFields();
    //             for (org.apache.lucene.index.IndexableField field : fields) {
    //                 if ("_source".equals(field.name())){
    //                     String source_string = source_bytes.utf8ToString();
    //                     System.out.println("Field name: " + field.name() + ", Field value: " + source_string);
    //                 } else if ("_id".equals(field.name())){
    //                     String uid = Uid.decodeId(document.getBinaryValue(field.name()).bytes);
    //                     System.out.println("Field name: " + field.name() + ", Field value: " + uid);
    //                 } else {
    //                     System.out.println("Field name: " + field.name());
    //                 }
    //             }
    //             System.out.println("--------------------------------");
    //         }
    //     }
    // }
}