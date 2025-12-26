package org.example;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.NotDirectoryException;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private static final List<File> files = new ArrayList<>();
    private static final int MAX_ROWS_PER_FILE = 600_000;

    private static Workbook workbook;
    private static Sheet sheet;
    private static FileOutputStream fos;

    private static int globalRowIndex = 0;
    private static int fileIndex = 1;
    private static boolean headerWritten = false;

    private static String outputDir;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println(
                    "Programa deve ser executado : java -jar <arquivo.jar> <diretorio dos csvs> <diretorio do destino>"
            );
            return;
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

        csvToXlsx(new File(outputFolder));

        System.out.println("XLSX created successfully.");
    }

    private static void verifyDirectory(String path) throws NotDirectoryException {
        File file = new File(path);
        if (!file.isDirectory()) {
            throw new NotDirectoryException(path);
        }
    }

    private static void loadFiles(String folder) {
        File dir = new File(folder);
        File[] list = dir.listFiles();

        if (list == null) return;

        for (File f : list) {
            if (f.isFile() && f.getName().endsWith(".csv")) {
                files.add(f);
            }
        }
    }

    private static void createNewWorkbook() throws IOException {
        workbook =new SXSSFWorkbook(100);
        sheet = workbook.createSheet("data");

        fos = new FileOutputStream(
                new File(outputDir, "output_" + fileIndex++ + ".xlsx")
        );

        globalRowIndex = 0;
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

            for (File csvFile : files) {
                processCsvFile(csvFile);
            }

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

                // Rotate file BEFORE exceeding limit
                if (globalRowIndex >= MAX_ROWS_PER_FILE) {
                    closeWorkbook();
                    createNewWorkbook();
                }

                // Header handling
                if (record.getRecordNumber() == 1) {
                    if (headerWritten) {
                        continue;
                    }
                    headerWritten = true;
                }

                Row row = sheet.createRow(globalRowIndex++);

                for (int i = 0; i < record.size(); i++) {
                    row.createCell(i).setCellValue(record.get(i));
                }
            }

        } catch (IOException e) {
            throw new RuntimeException("Error processing file: " + csvFile.getName(), e);
        }
    }
}
