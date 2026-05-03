package com.example.drm.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;

public class WriteAheadLog {
    private static final Logger log = LoggerFactory.getLogger(WriteAheadLog.class);
    private final Path logFile;
    private final Object lock = new Object();

    public WriteAheadLog(String nodeId) {
        this.logFile = Paths.get("wal_" + nodeId + ".log");
        try {
            Files.createFile(logFile);
        } catch (FileAlreadyExistsException ignored) {
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void append(long seq, String key, String value) {
        synchronized (lock) {
            try (BufferedWriter writer = Files.newBufferedWriter(logFile,
                    StandardOpenOption.APPEND, StandardOpenOption.CREATE)) {
                writer.write(String.format("%d|%s|%s%n", seq, key, value));
                writer.flush();
            } catch (IOException e) {
                log.error("WAL append error", e);
            }
        }
    }

    public void replay(java.util.function.BiConsumer<String, String> applier) {
        synchronized (lock) {
            try (BufferedReader reader = Files.newBufferedReader(logFile)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\|", 3);
                    if (parts.length == 3) {
                        applier.accept(parts[1], parts[2]);
                    }
                }
            } catch (IOException e) {
                log.error("WAL replay error", e);
            }
        }
    }
}