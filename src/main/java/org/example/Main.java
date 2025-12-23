package org.example;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


import java.io.*;
import java.nio.file.NotDirectoryException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    private static final List<File> files = new ArrayList<>();

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Programa deve ser executado : java - jar <arquivo.jar> <diretorio dos csvs> <diretorio do destino>");
            Thread.currentThread().interrupt();
        }

        String inputFolder = args[0];
        String outputFolder = args[1];

        try {
            verifyDirectory(inputFolder);
            verifyDirectory(outputFolder);
        } catch (NotDirectoryException e) {
            System.out.println(e.getMessage());
            return;
        }

        loadFiles(inputFolder);

        if (files.isEmpty()) {
            System.out.println("No CSV files found.");
            return;
        }

        File outputFile = new File(outputFolder, "output.xlsx");
        csvToXlsx(outputFile);

        System.out.println("XLSX created successfully: " + outputFile.getAbsolutePath());
    }

    private static void verifyDirectory(String path) throws NotDirectoryException {
        File file = new File(path);
        if (!file.isDirectory()) {
            throw new NotDirectoryException(path);
        }
    }

    private static void loadFiles(String folder) {
        File dir = new File(folder);
        for (File f : dir.listFiles()) {
            if (f != null && f.getName().endsWith(".csv")) {
                files.add(f);
            }
        }
    }

    private static void csvToXlsx(File outputFile) {

        try (
                Workbook workbook = new XSSFWorkbook();
                FileOutputStream fos = new FileOutputStream(outputFile)
        ) {
            Sheet sheet = workbook.createSheet("data");

            ExecutorService executor =
                    Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

            AtomicInteger rowIndex = new AtomicInteger(0);
            AtomicBoolean headerWritten = new AtomicBoolean(false);

            for (File csvFile : files) {
                executor.submit(() -> processCsvFile(csvFile, sheet, rowIndex, headerWritten));
            }

            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.HOURS);

            workbook.write(fos);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void processCsvFile(
            File csvFile,
            Sheet sheet,
            AtomicInteger rowIndex,
            AtomicBoolean headerWritten
    ) {

        try (Reader reader = new FileReader(csvFile)) {

            Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(reader);

            for (CSVRecord record : records) {

                SheetWriteLock.LOCK.lock();
                try {
                    // Header handling (atomic)
                    if (record.getRecordNumber() == 1) {
                        if (headerWritten.get()) {
                            continue; // skip header
                        }
                        headerWritten.set(true); // first header wins
                    }

                    int currentRow = rowIndex.getAndIncrement();
                    Row row = sheet.createRow(currentRow);

                    for (int i = 0; i < record.size(); i++) {
                        row.createCell(i).setCellValue(record.get(i));
                    }

                } finally {
                    SheetWriteLock.LOCK.unlock();
                }
            }

        } catch (IOException e) {
            throw new RuntimeException("Error processing file: " + csvFile.getName(), e);
        }
    }



}
