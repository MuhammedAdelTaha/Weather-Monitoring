package org.example;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class BitCaskStore {
    private static final String DATA_FILE = "bitcask.data";
    private static final String HINT_FILE = "bitcask.hint";

    private RandomAccessFile dataFile;
    private Map<String, Long> index = new HashMap<>();

    public BitCaskStore() throws IOException {
        dataFile = new RandomAccessFile(DATA_FILE, "rw");
        recover();
    }

    public synchronized void put(String key, String value) throws IOException {
        dataFile.seek(dataFile.length());
        long pos = dataFile.getFilePointer();

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        // Format: key length (int) | key bytes | value length (int) | value bytes
        dataFile.writeInt(keyBytes.length);
        dataFile.write(keyBytes);
        dataFile.writeInt(valueBytes.length);
        dataFile.write(valueBytes);

        // Update in-memory index
        index.put(key, pos);

        // Update hint file (simulate by rewriting)
        writeHintFile();
    }

    public synchronized String get(String key) throws IOException {
        Long pos = index.get(key);
        if (pos == null) return null;

        dataFile.seek(pos);

        int keyLen = dataFile.readInt();
        byte[] keyBytes = new byte[keyLen];
        dataFile.readFully(keyBytes);
        String storedKey = new String(keyBytes, StandardCharsets.UTF_8);

        int valueLen = dataFile.readInt();
        byte[] valueBytes = new byte[valueLen];
        dataFile.readFully(valueBytes);
        String value = new String(valueBytes, StandardCharsets.UTF_8);

        if (!storedKey.equals(key)) {
            throw new IOException("Key mismatch at position");
        }
        return value;
    }

    private void writeHintFile() throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(HINT_FILE))) {
            for (Map.Entry<String, Long> e : index.entrySet()) {
                writer.write(e.getKey() + "," + e.getValue());
                writer.newLine();
            }
        }
    }

    private void recover() throws IOException {
        File hint = new File(HINT_FILE);
        if (!hint.exists()) return;

        try (BufferedReader reader = new BufferedReader(new FileReader(hint))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length != 2) continue;
                String key = parts[0];
                Long pos = Long.parseLong(parts[1]);
                index.put(key, pos);
            }
        }
    }

    public synchronized Map<String, String> getAll() throws IOException {
        Map<String, String> all = new HashMap<>();
        for (String key : index.keySet()) {
            all.put(key, get(key));
        }
        return all;
    }
}
