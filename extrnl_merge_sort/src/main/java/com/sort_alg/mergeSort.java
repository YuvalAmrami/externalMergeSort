package com.sort_alg;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class mergeSort {

    private  int MAX_MEMORY;
    private  ExecutorService pool;
    private  int keyIndex ;
    private String FileName;
    private String FileNameOut;

    mergeSort(int maxMem,int poolSize, int keyIndexInput, String FileNameInput){
        MAX_MEMORY = maxMem;
        pool = Executors.newFixedThreadPool(poolSize);
        keyIndex = keyIndexInput;
        FileName = FileNameInput;
        FileNameOut = "sorted"+FileNameInput;
    }
    


    // public  void sortCSV(String inputFile, String outputFile) throws IOException {
    public  void sortCSV() throws IOException {
        List<File> runs = splitCSV(FileName); 
        List<BufferedReader> results;
        
        try {
            results = readRunsParallel(runs);
            
            BufferedWriter writer = new BufferedWriter(new FileWriter(FileNameOut));
            mergeRuns(results, writer);
            writer.close();

        } finally { 
            cleanup(runs);
            pool.shutdown();
        }
    }

    
    private  List<File> splitCSV(String inputFile) throws IOException {
        
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        String header = reader.readLine();
        
        List<File> runs = new ArrayList<>();
        List<String> lines = new ArrayList<>();
        
        String line = reader.readLine();
        while (line != null) {
            
            lines.add(line);
            
            if (getSize(lines) >= MAX_MEMORY) {
                File file = writeRun(lines, header); 
                runs.add(file);
                lines.clear();
            }
            
            line = reader.readLine();
        }
        
        if (!lines.isEmpty()) {
            File file = writeRun(lines, header);
            runs.add(file);
        }
        reader.close();
        return runs;
    }
    
    private  int getSize(List<String> run) {
        int size = 0;
        for(String line : run) {
            size += line.length();
        }
        return size;
    }

    private  List<BufferedReader> readRunsParallel(List<File> runs) throws IOException {
    
        List<BufferedReader> results = new ArrayList<>();
        for(File file : runs) {
            try {
                results.add(new BufferedReader((new FileReader(file))));
            } catch(FileNotFoundException e) {}
        }
        return results;
    }


    private  File writeRun(List<String> lines, String header) throws IOException {
        File file = File.createTempFile("run", ".csv"); 
        
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write(header);
        writer.newLine();
        
        for(String line : lines) {
            writer.write(line);
            writer.newLine();
        }
        writer.close();
        
        return file;
    }
    




    private  void mergeRuns(List<BufferedReader> readers, Writer writer) throws IOException {

        PriorityQueue<CSVEntry> minHeap = new PriorityQueue<>(readers.size());
        
        for(BufferedReader reader : readers) {
            String line = reader.readLine();
            if(line != null) {
                String[] parts = line.split(",");
                int key = Integer.parseInt(parts[keyIndex]); 
                minHeap.add(new CSVEntry(key, line, reader));    
            }
        }
        
        while(!minHeap.isEmpty()) {
           
            CSVEntry entry = minHeap.poll();
            writer.write(entry.line); 
            writer.write("\n");
    
            String nextLine = entry.reader.readLine();
            if(nextLine != null) {
                String[] parts = nextLine.split(",");
                int key = Integer.parseInt(parts[0]);
                minHeap.add(new CSVEntry(key, nextLine, entry.reader));
            }
        }
    }
    

    private  void cleanup(List<File> runs) {
        for(File file : runs) {
            file.delete(); 
        }
    }

}