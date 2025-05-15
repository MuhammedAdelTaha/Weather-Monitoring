package org.example;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.StreamSupport;

public class BitCask {
    private static final int MAX_FILE_SIZE = 1024 * 1024 * 10; // 10 MB per segment file
    private final Path dataDir;
    private final Map<String, KeyDirEntry> keyDir = new ConcurrentHashMap<>();
    private final ScheduledExecutorService compactionScheduler = Executors.newScheduledThreadPool(1);
    private RandomAccessFile currentFile;
    private int currentFileIndex;
    private final Object fileLock = new Object();

    // Entry in the keyDir with file information
    private static class KeyDirEntry {
        final int fileIndex;
        final long position;

        KeyDirEntry(int fileIndex, long position) {
            this.fileIndex = fileIndex;
            this.position = position;
        }
    }

    public BitCask(String dir) {
        this.dataDir = Paths.get(dir);
        try {
            if (!Files.exists(dataDir)) {
                Files.createDirectories(dataDir);
            }

            // Load existing data into keyDir
            loadExistingData();

            // Open the current segment for writing
            currentFileIndex = getLatestFileIndex();
            openNewSegment();

            // Schedule compaction
            scheduleCompaction();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize BitCask", e);
        }
    }

    private void loadExistingData() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "segment_*.log")) {
            for (Path path : stream) {
                String fileName = path.getFileName().toString();
                int fileIndex = Integer.parseInt(fileName.replace("segment_", "").replace(".log", ""));

                try (BufferedReader reader = Files.newBufferedReader(path)) {
                    String line;
                    long position = 0;
                    while ((line = reader.readLine()) != null) {
                        int commaPos = line.indexOf(',');
                        if (commaPos > 0) {
                            String key = line.substring(0, commaPos);
                            keyDir.put(key, new KeyDirEntry(fileIndex, position));
                        }
                        position += line.length() + 1; // +1 for newline
                    }
                }
            }
        }
    }

    private int getLatestFileIndex() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "segment_*.log")) {
            return StreamSupport.stream(stream.spliterator(), false)
                    .map(path -> Integer.parseInt(path.getFileName().toString().replace("segment_", "").replace(".log", "")))
                    .max(Integer::compare)
                    .orElse(0);
        }
    }

    private void openNewSegment() throws IOException {
        synchronized (fileLock) {
            if (currentFile != null) {
                currentFile.close();
            }
            currentFile = new RandomAccessFile(dataDir.resolve("segment_" + (++currentFileIndex) + ".log").toFile(), "rw");
            currentFile.seek(currentFile.length()); // Move to end of file for appending
        }
    }

    public void put(String key, String value) {
        synchronized (fileLock) {
            try {
                String entry = key + "," + value + "\n";
                if (currentFile.length() + entry.length() > MAX_FILE_SIZE) {
                    openNewSegment();
                }
                long position = currentFile.length();
                currentFile.seek(position);
                currentFile.writeBytes(entry);
                keyDir.put(key, new KeyDirEntry(currentFileIndex, position));
            } catch (IOException e) {
                System.err.println("Failed to write data: " + e.getMessage());
            }
        }
    }

    public String get(String key) {
        KeyDirEntry entry = keyDir.get(key);
        if (entry == null) return null;

        // Try to read from the appropriate segment file
        try {
            Path filePath = dataDir.resolve("segment_" + entry.fileIndex + ".log");
            if (!Files.exists(filePath)) {
                return null; // File might have been removed during compaction
            }

            try (RandomAccessFile file = new RandomAccessFile(filePath.toFile(), "r")) {
                file.seek(entry.position);
                String line = file.readLine();
                if (line == null) return null;

                int commaPos = line.indexOf(',');
                if (commaPos < 0) return null;

                return line.substring(commaPos + 1);
            }
        } catch (IOException e) {
            System.err.println("Failed to read data: " + e.getMessage());
            return null;
        }
    }

    // Implement getAll method required by CentralStationServer
    public Map<String, String> getAll() {
        Map<String, String> result = new HashMap<>();
        for (String key : keyDir.keySet()) {
            String value = get(key);
            if (value != null) {
                result.put(key, value);
            }
        }
        return result;
    }

    private void scheduleCompaction() {
        compactionScheduler.scheduleAtFixedRate(this::compact, 60, 60, TimeUnit.SECONDS);
    }

    private void compact() {
        synchronized (fileLock) {
            try {
                System.out.println("Starting compaction...");
                Path compactedFile = dataDir.resolve("compacted.log");

                // Write all current key-value pairs to new file
                try (BufferedWriter writer = Files.newBufferedWriter(compactedFile)) {
                    for (String key : keyDir.keySet()) {
                        String value = get(key);
                        if (value != null) {
                            writer.write(key + "," + value + "\n");
                        }
                    }
                }

                // Close current file and collect old segment files
                currentFile.close();
                List<Path> oldSegments = new ArrayList<>();
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "segment_*.log")) {
                    for (Path path : stream) {
                        oldSegments.add(path);
                    }
                }

                // Move compacted file to new segment
                Files.move(compactedFile, dataDir.resolve("segment_" + (++currentFileIndex) + ".log"),
                        StandardCopyOption.REPLACE_EXISTING);

                // Update keyDir with new positions
                Map<String, KeyDirEntry> newKeyDir = new ConcurrentHashMap<>();
                long position = 0;

                try (BufferedReader reader = Files.newBufferedReader(
                        dataDir.resolve("segment_" + currentFileIndex + ".log"))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        int commaPos = line.indexOf(',');
                        if (commaPos > 0) {
                            String key = line.substring(0, commaPos);
                            newKeyDir.put(key, new KeyDirEntry(currentFileIndex, position));
                        }
                        position += line.length() + 1; // +1 for newline
                    }
                }

                // Replace keyDir with new positions
                keyDir.clear();
                keyDir.putAll(newKeyDir);

                // Open new segment file
                openNewSegment();

                // Delete old segment files
                for (Path oldSegment : oldSegments) {
                    try {
                        Files.deleteIfExists(oldSegment);
                    } catch (IOException e) {
                        System.err.println("Failed to delete old segment: " + oldSegment);
                    }
                }

                System.out.println("Compaction completed.");
            } catch (IOException e) {
                System.err.println("Compaction failed: " + e.getMessage());

                // Try to reopen current segment if compaction failed
                try {
                    openNewSegment();
                } catch (IOException reopenEx) {
                    System.err.println("Failed to reopen segment after compaction failure: " + reopenEx.getMessage());
                }
            }
        }
    }

    // Properly shut down resources
    public void shutdown() {
        compactionScheduler.shutdown();
        try {
            if (currentFile != null) {
                currentFile.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing files during shutdown: " + e.getMessage());
        }
    }
}