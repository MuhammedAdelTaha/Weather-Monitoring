package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.StreamSupport;

public class BitCask {
    private static final int MAX_FILE_SIZE = 1024 * 1024 * 10; // 10 MB
    private static final Logger logger = LoggerFactory.getLogger(BitCask.class);
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final Path dataDir;
    private final Map<String, KeyDirEntry> keyDir = new ConcurrentHashMap<>();
    private final ScheduledExecutorService compactionScheduler = Executors.newScheduledThreadPool(1);
    private RandomAccessFile currentFile;
    private RandomAccessFile currentHintFile; // Added for hint files
    private int currentFileIndex;
    private final Object fileLock = new Object();

    private record KeyDirEntry(int fileIndex, long position, int valueSize) { }

    public BitCask(String dir) throws IOException {
        this.dataDir = Paths.get(dir);
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        }
        loadExistingData();
        currentFileIndex = getLatestFileIndex();
        openSegment(false);
        scheduleCompaction();
    }

    private void loadExistingData() throws IOException {
        // First try to load from hint files (faster)
        logger.info("[{}] Starting to load from hint files.", LocalDateTime.now().format(dtf));
        try (DirectoryStream<Path> hintStream = Files.newDirectoryStream(dataDir, "segment_*.hint")) {
            for (Path hintPath : hintStream) {
                try (BufferedReader reader = Files.newBufferedReader(hintPath)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split(",");
                        if (parts.length >= 4) {
                            String key = parts[0];
                            int fileIndex = Integer.parseInt(parts[1]);
                            long position = Long.parseLong(parts[2]);
                            int valueSize = Integer.parseInt(parts[3]);
                            keyDir.put(key, new KeyDirEntry(fileIndex, position, valueSize));
                        }
                    }
                }
            }
        }

        // Fallback to scanning data files if no hints exist
        if (keyDir.isEmpty()) {
            logger.warn("[{}] No data loaded from hint files. Falling back to scanning log files.",LocalDateTime.now().format(dtf));
            try (DirectoryStream<Path> dataStream = Files.newDirectoryStream(dataDir, "segment_*.log")) {
                for (Path dataPath : dataStream) {
                    String fileName = dataPath.getFileName().toString();
                    int fileIndex = Integer.parseInt(fileName.replace("segment_", "").replace(".log", ""));

                    try (BufferedReader reader = Files.newBufferedReader(dataPath)) {
                        String line;
                        long position = 0;
                        while ((line = reader.readLine()) != null) {
                            int commaPos = line.indexOf(',');
                            if (commaPos > 0) {
                                String key = line.substring(0, commaPos);
                                String value = line.substring(commaPos + 1);
                                keyDir.put(key, new KeyDirEntry(fileIndex, position, value.length()));
                            }
                            position += line.length() + 1; // +1 for newline
                        }
                    }
                }
            }
        }
        else{
            logger.info("[{}] data loaded from hint files.",LocalDateTime.now().format(dtf));
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

    // create segment and hint files
    private void openSegment(boolean openNewFile) throws IOException {
        synchronized (fileLock) {
            if (currentFile != null) currentFile.close();
            if (currentHintFile != null) currentHintFile.close();

            // Increment file index to open a new file
            if (openNewFile) {
                currentFileIndex++;
            }

            Path dataPath = dataDir.resolve("segment_" + currentFileIndex + ".log");
            Path hintPath = dataDir.resolve("segment_" + currentFileIndex + ".hint");

            currentFile = new RandomAccessFile(dataPath.toFile(), "rw");
            currentHintFile = new RandomAccessFile(hintPath.toFile(), "rw");
            currentFile.seek(currentFile.length());
            currentHintFile.seek(currentHintFile.length());
        }
    }

    // Modified to write hint entries
    public void put(String key, String value) {
        synchronized (fileLock) {
            try {
                String entry = key + "," + value + "\n";
                if (currentFile.length() + entry.length() > MAX_FILE_SIZE) {
                    openSegment(true);
                }

                long position = currentFile.length();
                currentFile.seek(position);
                currentFile.writeBytes(entry);

                // Write to a hint file
                String hintEntry = key + "," + currentFileIndex + "," + position + "," + value.length() + "\n";
                currentHintFile.seek(currentHintFile.length());
                currentHintFile.writeBytes(hintEntry);

                keyDir.put(key, new KeyDirEntry(currentFileIndex, position, value.length()));
            } catch (IOException e) {
                logger.error("Failed to write data: {}", e.getMessage());
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
            logger.error("Failed to read data: {}", e.getMessage());
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

    // Modified compaction to handle hint files

    private void compact() {
        synchronized (fileLock) {
            try {
                logger.info("[{}] Starting compaction...", LocalDateTime.now().format(dtf));
                int newFileIndex = currentFileIndex + 1;
                Path tempData = Files.createTempFile(dataDir, "compacted_", ".log");
                Path tempHint = Files.createTempFile(dataDir, "compacted_", ".hint");

                try (RandomAccessFile newDataFile = new RandomAccessFile(tempData.toFile(), "rw");
                     RandomAccessFile newHintFile = new RandomAccessFile(tempHint.toFile(), "rw")) {

                    for (Map.Entry<String, KeyDirEntry> entry : keyDir.entrySet()) {
                        String key = entry.getKey();
                        String value = get(key);
                        if (value != null) {
                            // Write to a new data file
                            String dataEntry = key + "," + value + "\n";
                            long position = newDataFile.getFilePointer();
                            newDataFile.writeBytes(dataEntry);

                            // Update keyDir and write to a hint file
                            KeyDirEntry newEntry = new KeyDirEntry(newFileIndex, position, value.length());
                            keyDir.put(key, newEntry);

                            String hintEntry = key + "," + newFileIndex + "," + position + "," + value.length() + "\n";
                            newHintFile.writeBytes(hintEntry);
                        }
                    }
                }

                // Close current files
                currentFile.close();
                currentHintFile.close();

                // Rename temp files to new segment files
                Path newDataPath = dataDir.resolve("segment_" + newFileIndex + ".log");
                Path newHintPath = dataDir.resolve("segment_" + newFileIndex + ".hint");
                Files.move(tempData, newDataPath, StandardCopyOption.REPLACE_EXISTING);
                Files.move(tempHint, newHintPath, StandardCopyOption.REPLACE_EXISTING);

                // Delete old files
                try (DirectoryStream<Path> oldFiles = Files.newDirectoryStream(dataDir,
                        path -> {
                            String name = path.getFileName().toString();
                            return name.matches("segment_\\d+\\.(log|hint)") &&
                                    !name.equals(newDataPath.getFileName().toString()) &&
                                    !name.equals(newHintPath.getFileName().toString());
                        })) {
                    for (Path oldFile : oldFiles) {
                        Files.delete(oldFile);
                    }
                }

                // Update current file index and open new segment
                currentFileIndex = newFileIndex;
                openSegment(false);
                logger.info("[{}] compaction completed...", LocalDateTime.now().format(dtf));
            } catch (IOException e) {
                logger.error("Compaction failed: {}", e.getMessage());
                try {
                    openSegment(false);
                } catch (IOException ex) {
                    logger.error("Failed to reopen segment: {}", ex.getMessage());
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
            logger.error("Error closing files during shutdown: {}", e.getMessage());
        }
    }
}