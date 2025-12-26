package org.example;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


import java.io.*;
import java.nio.charset.StandardCharsets;
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
    private static final int MAX_ROWS_PER_FILE = 600_000;
    private static Workbook workbook;
    private static Sheet sheet;
    private static FileOutputStream fos;

    private static final AtomicInteger globalRowIndex = new AtomicInteger(0);
    private static final AtomicInteger fileIndex = new AtomicInteger(1);
    private static boolean headerWritten = false;

    private static String outputDir;


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

        File outputFile = new File(outputFolder);
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

    private static void createNewWorkbook() throws IOException {
        workbook = new XSSFWorkbook();
        sheet = workbook.createSheet("data");
        fos = new FileOutputStream(
                new File(outputDir, "output_" + fileIndex.getAndIncrement() + ".xlsx")
        );

        globalRowIndex.set(0);
        headerWritten = false;
    }

    private static void closeWorkbook() throws IOException {
        workbook.write(fos);
        fos.close();
        workbook.close();
    }


    private static void csvToXlsx(File outputFolder) {

        outputDir = outputFolder.getAbsolutePath();

        try {
            createNewWorkbook();

            ExecutorService executor =
                    Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

            for (File csvFile : files) {
                executor.submit(() -> processCsvFile(csvFile));
            }

            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.HOURS);

            closeWorkbook();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static void processCsvFile(File csvFile) {

        try (Reader reader = new InputStreamReader(
                new FileInputStream(csvFile),
                StandardCharsets.UTF_16LE
        )) {

            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setDelimiter('\t')
                    .setTrim(true)
                    .setIgnoreEmptyLines(true)
                    .get();

            Iterable<CSVRecord> records = csvFormat.parse(reader);

            for (CSVRecord record : records) {

                SheetWriteLock.LOCK.lock();
                try {
                    // Rotate file BEFORE exceeding limit
                    if (globalRowIndex.get() >= MAX_ROWS_PER_FILE) {
                        closeWorkbook();
                        createNewWorkbook();
                    }

                    // Header logic (per file)
                    if (record.getRecordNumber() == 1) {
                        if (headerWritten) {
                            continue;
                        }
                        headerWritten = true;
                    }

                    int rowNum = globalRowIndex.getAndIncrement();
                    Row row = sheet.createRow(rowNum);

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
