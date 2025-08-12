import java.io.*;
import java.nio.file.*;
import java.util.*;

public class JavaEtl {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java javaEtl.java input.csv output.csv");
            System.exit(1);
        }

        String inputFile = args[0];
        String outputFile = args[1];

        System.out.println("Starting Java ETL with CSV parsing...");
        System.out.println("Input: " + inputFile);
        System.out.println("Output: " + outputFile);

        long startTime = System.currentTimeMillis();

        try {
            processCsvAdvanced(inputFile, outputFile);

            long endTime = System.currentTimeMillis();
            double duration = (endTime - startTime) / 1000.0;
            System.out.printf("Java Advanced ETL completed in %.2f seconds%n", duration);

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void processCsvAdvanced(String inputFile, String outputFile) throws IOException {
        long processedCount = 0;
        long eligibleCount = 0;
        long invalidCount = 0;
        int electricRangeIndex = -1;

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(inputFile));
             BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile))) {

            String headerLine = reader.readLine();
            if (headerLine == null) {
                throw new IOException("Empty file");
            }

            List<String> header = parseCsvLine(headerLine);
            writer.write(String.join(",", header));
            writer.newLine();

            for (int i = 0; i < header.size(); i++) {
                if (header.get(i).trim().equals("Electric Range")) {
                    electricRangeIndex = i;
                    break;
                }
            }

            if (electricRangeIndex == -1) {
                throw new IOException("Column 'Electric Range' not found");
            }

            String line;
            while ((line = reader.readLine()) != null) {
                processedCount++;

                try {
                    List<String> fields = parseCsvLine(line);

                    if (electricRangeIndex >= fields.size()) {
                        invalidCount++;
                        continue;
                    }

                    String electricRangeStr = fields.get(electricRangeIndex).trim();
                    if (electricRangeStr.isEmpty()) {
                        invalidCount++;
                        continue;
                    }

                    int electricRange = Integer.parseInt(electricRangeStr);

                    if (electricRange > 200) {
                        writer.write(String.join(",", fields));
                        writer.newLine();
                        eligibleCount++;
                    }

                } catch (NumberFormatException e) {
                    invalidCount++;
                } catch (Exception e) {
                    invalidCount++;
                }

                if (processedCount % 100000 == 0) {
                    System.out.println("Processed " + processedCount + " records...");
                }
            }
        }

        System.out.println("Total processed: " + processedCount + " records");
        System.out.println("Eligible: " + eligibleCount + " records");
        System.out.println("Invalid/Skipped: " + invalidCount + " records");
    }

    private static List<String> parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        boolean inQuotes = false;
        StringBuilder currentField = new StringBuilder();

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"' && (i == 0 || line.charAt(i-1) != '\\')) {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                fields.add(currentField.toString().trim());
                currentField = new StringBuilder();
            } else {
                currentField.append(c);
            }
        }

        fields.add(currentField.toString().trim());
        return fields;
    }
}