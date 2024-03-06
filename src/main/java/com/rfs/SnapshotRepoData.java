package com.rfs;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SnapshotRepoData {
    private static Path findRepoFile(Path dir) throws IOException {
        // The directory may contain multiple of these files, but we want the highest versioned one
        Pattern pattern = Pattern.compile("^index-(\\d+)$");
        Path highestVersionedFile = null;
        int highestVersion = -1;
        
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                if (Files.isRegularFile(entry)) {
                    Matcher matcher = pattern.matcher(entry.getFileName().toString());
                    if (matcher.matches()) {
                        int version = Integer.parseInt(matcher.group(1));
                        if (version > highestVersion) {
                            highestVersion = version;
                            highestVersionedFile = entry;
                        }
                    }
                }
            }
        }
        return highestVersionedFile;
    }

    public static SnapshotRepoData fromRepoFile(Path filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SnapshotRepoData data = mapper.readValue(new File(filePath.toString()), SnapshotRepoData.class);
        data.filePath = filePath;
        return data;
    }

    public static SnapshotRepoData fromRepoDir(Path dir) throws IOException {
        Path file = findRepoFile(dir);
        if (file == null) {
            throw new IOException("No index file found in " + dir);
        }
        return fromRepoFile(file);
    }

    public Path filePath;
    public List<Snapshot> snapshots;
    public Map<String, RawIndex> indices;

    public static class Snapshot {
        public String name;
        public String uuid;
        public int state;
    }

    public static class RawIndex {
        public String id;
        public List<String> snapshots;
    }

    public static class Index {
        public static Index fromRawIndex(String name, RawIndex rawIndex) {
            Index index = new Index();
            index.name = name;
            index.id = rawIndex.id;
            index.snapshots = rawIndex.snapshots;
            return index;
        }

        public String name;
        public String id;
        public List<String> snapshots;
    }
}

