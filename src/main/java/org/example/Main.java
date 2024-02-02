package org.example;

import java.io.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    public static void main(String[] args) {
        String input = args[0];
        String output = "missing" + args[1];

        // Fixed file location of the large CSV file
        Long startId = Long.valueOf(args[2]); // Define the starting ID for exporting data
        int rowsPerChunk = 100_000; // Define the number of rows per chunk
        int numberOfThreads = 8; // Adjust the number of threads as needed

        try {
            BufferedReader reader = new BufferedReader(new FileReader(input));
            String header = reader.readLine(); // Read and store the header

            String line;
            Long currentId = 0L;
            ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);

            // Find the starting ID
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(";");
                if (parts.length > 0 && parts[0].matches("\\d+")) {
                    currentId = Long.parseLong(parts[0]);
                    if (currentId >= startId) {
                        break;
                    }
                }
            }

            if (currentId < startId) {
                System.out.println("Starting ID not found.");
                return;
            }

            int fileNumber = 1;

            while ((line = reader.readLine()) != null) {
                String chunkFilePath = output + fileNumber + ".csv";
                BufferedWriter writer = new BufferedWriter(new FileWriter(chunkFilePath));

                writer.write(header); // Write the header to each chunk file
                writer.newLine();

                for (int i = 0; i < rowsPerChunk && line != null; i++) {
                    writer.write(line);
                    writer.newLine();
                    line = reader.readLine();
                }

                writer.close();
                Callable<String> task = () -> "Chunk " + chunkFilePath + " is created";
                executor.submit(task);
                fileNumber++;
            }

            reader.close();
            executor.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
