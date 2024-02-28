package com.rfs;

import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;


import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;;

public class LuceneDocumentReader {

    public LuceneDocumentReader() {
        // Add any initialization code here
    }

    public static void main(String[] args) {
        // Paths
        String snapshotDirectoryPath = "/Users/chelma/workspace/ElasticSearch/elasticsearch/build/testclusters/runTask-0/repo/snapshots";
        SnapshotId snapshotId = new SnapshotId("my_snapshot", "Q-cTkU6-R2ijMoEAwBCouQ");
        String indexDirectoryPath = "/Users/chelma/workspace/ElasticSearch/elasticsearch/build/testclusters/runTask-0/data/nodes/0/indices/yflN6Nk6TVioQccGgHU1UQ/0/index";

        try {
            System.out.println("Attempting to read Snapshot details...");
            readSnapshotDetails(snapshotDirectoryPath, snapshotId);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            System.out.println("Starting document reads...");
            readDocumentsFromLuceneIndex(indexDirectoryPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void readSnapshotDetails(String snapshotDirectoryPath, SnapshotId snapshotId) throws Exception {
        Path snapshotPath = Paths.get(snapshotDirectoryPath);
        BlobPath blobPath = new BlobPath();
        blobPath.add(snapshotDirectoryPath);

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

        System.out.println("Snapshot details: " + snapshotInfo.toString());

        blobStore.close();
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
                System.out.println("--------------------------------");
            }
        }
    }
}