package org.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ParquetArchiver {
    private static final Logger logger = LoggerFactory.getLogger(ParquetArchiver.class);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final int BATCH_SIZE = 1000;

    private final BlockingQueue<GenericRecord> queue = new LinkedBlockingQueue<>();
    private final List<GenericRecord> batchBuffer = new ArrayList<>(BATCH_SIZE);
    private final String outputDir;
    private final Schema schema;
    private volatile boolean running = true;

    public ParquetArchiver(String outputDir, Schema schema) {
        this.outputDir = outputDir;
        this.schema = schema;
        createDirectory(outputDir);
        startBatchWorker();
    }

    private void createDirectory(String path) {
        try {
            Files.createDirectories(Paths.get(path));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create archive directory", e);
        }
    }

    private void startBatchWorker() {
        Thread workerThread = new Thread(() -> {
            while (running || !queue.isEmpty()) {
                try {
                    GenericRecord record = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (record != null) {
                        batchBuffer.add(record);
                        logger.info("Added a parquet record and size of the batch is {}",batchBuffer.size());
                    }

                    if (batchBuffer.size() >= BATCH_SIZE || (!running && !batchBuffer.isEmpty())) {
                        writeBatch(new ArrayList<>(batchBuffer));
                        batchBuffer.clear();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Batch worker thread interrupted");
                } catch (Exception e) {
                    logger.error("Error in batch worker thread", e);
                }
            }
        }, "parquet-archiver-worker");

        logger.info("Batch worker thread Started");
        workerThread.setDaemon(true);
        workerThread.start();
    }

    public void archive(GenericRecord record) {
        if (!queue.offer(record)) {
            logger.warn("Failed to add record to queue - queue full");
        }
    }

    private void writeBatch(List<GenericRecord> batch) {
        if (batch.isEmpty()) {
            return;
        }

        logger.info("Starting to write a batch...");
        // Group records by partitioning attributes (timestamp and station ID)
        Map<String, List<GenericRecord>> groupedBatches = batch.stream()
                .collect(Collectors.groupingBy(this::getPartitionKey));

        for (Map.Entry<String, List<GenericRecord>> entry : groupedBatches.entrySet()) {
            List<GenericRecord> groupedBatch = entry.getValue();
            Path filePath = buildFilePath(groupedBatch.get(0));

            try {
                Files.createDirectories(filePath.getParent());
                try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(filePath.toFile()))
                        .withSchema(schema)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .build()) {
                    for (GenericRecord record : groupedBatch) {
                        writer.write(record);
                    }
                    logger.info("Successfully wrote batch of {} records to {}", groupedBatch.size(), filePath);
                }
            } catch (IOException e) {
                logger.error("Failed to write batch to {}", filePath, e);
            }
        }
    }

    private String getPartitionKey(GenericRecord record) {
        long timestamp = (Long) record.get("status_timestamp");
        LocalDateTime dt = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        String dateStr = dt.format(DATE_FORMAT);
        int hour = dt.getHour();
        long stationId = (Long) record.get("station_id");
        return "date=" + dateStr + "/hour=" + hour + "/station_id=" + stationId;
    }

    private Path buildFilePath(GenericRecord record) {
        long timestamp = (Long) record.get("status_timestamp");
        LocalDateTime dt = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        String dateStr = dt.format(DATE_FORMAT);
        int hour = dt.getHour();
        long stationId = (Long) record.get("station_id");

        return Paths.get(outputDir,
                "date=" + dateStr,
                "hour=" + hour,
                "station_id=" + stationId,
                "data_" + System.currentTimeMillis() + ".parquet");
    }


    public void shutdown() {
        running = false;
    }

    static class LocalOutputFile implements OutputFile {
        private final File file;

        public LocalOutputFile(File file) {
            this.file = file;
        }

        @Override
        public PositionOutputStream create(long blockSize) throws IOException {
            return new LocalPositionOutputStream(file, false);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSize) throws IOException {
            return new LocalPositionOutputStream(file, true);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }

    static class LocalPositionOutputStream extends PositionOutputStream {
        private final OutputStream out;
        private long position = 0;

        public LocalPositionOutputStream(File file, boolean overwrite) throws IOException {
            this.out = new FileOutputStream(file, !overwrite);
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            position++;
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
            position += b.length;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            position += len;
        }

        @Override
        public void close() throws IOException {
            out.close();
        }

        @Override
        public long getPos() throws IOException {
            return position;
        }
    }

    public Schema getSchema() {
        return schema;
    }

}