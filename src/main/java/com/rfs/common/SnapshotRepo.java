package com.rfs.common;

import java.nio.file.Path;
import java.util.List;

public class SnapshotRepo {
    public static interface Provider {
        // Returns the path to the snapshot directory
        public Path getSnapshotDirPath();
    
        // Returns a list of all snapshots in the snapshot repository
        public List<SnapshotRepo.Snapshot> getSnapshots();
    
        // Returns a list of all indices in the specified snapshot
        public List<SnapshotRepo.Index> getIndicesInSnapshot(String snapshotName);
        
        // Get the ID of a snapshot from its name
        public String getSnapshotId(String snapshotName);
    
        // Get the ID of an index from its name
        public String getIndexId(String indexName);
        
    }

    public static interface Snapshot {
        String getName();
        String getId();
    }

    public static interface Index {
        String getName();
        String getId();
        List<String> getSnapshots();
    }

}


