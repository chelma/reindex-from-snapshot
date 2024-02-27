package main.java.com.rfs;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import java.nio.file.Paths;
import java.util.List;

public class LuceneDocumentReader {

    public static void main(String[] args) {
        // Replace this with the path to your Lucene index directory
        String indexDirectoryPath = "/tmp/data/nodes/0/indices/qS8W6q9JTVuWiuQeJrPfbQ/0/index";

        try {
            System.out.println("Starting reads...");
            readDocumentsFromIndex(indexDirectoryPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void readDocumentsFromIndex(String indexDirectoryPath) throws Exception {
        // Opening the directory that contains the Lucene index
        try (FSDirectory directory = FSDirectory.open(Paths.get(indexDirectoryPath));
             IndexReader reader = DirectoryReader.open(directory)) {

            // Iterating over all documents in the index
            for (int i = 0; i < reader.maxDoc(); i++) {
                System.out.println("Reading Document");
                if (!reader.hasDeletions()) { // Skip deleted documents
                    Document document = reader.document(i);

                    // Iterate over all fields in the document
                    List<org.apache.lucene.index.IndexableField> fields = document.getFields();
                    for (org.apache.lucene.index.IndexableField field : fields) {
                        System.out.println("Field name: " + field.name() + ", Field value: " + document.getBinaryValue(field.name()));
                    }
                    System.out.println("--------------------------------");
                }
            }
        }
    }
}