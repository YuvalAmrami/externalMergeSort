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
    private String currDir = System.getProperty("user.dir");
    private Boolean isTest = false;

    public mergeSort(int maxMem,int poolSize, int keyIndexInput, String FileNameInput, String dir, Boolean... isTest ){
        this.MAX_MEMORY = maxMem;
        if(Runtime.getRuntime().availableProcessors()>=poolSize){
            this.pool = Executors.newFixedThreadPool(poolSize);
        } else{
            System.err.println("the pool size is bigger than the number of available runtime processors so the number of threads will be 1");
            this.pool = Executors.newFixedThreadPool(1);
        }
        this.keyIndex = keyIndexInput;
        this.FileName = dir+"/"+FileNameInput;
        this.FileNameOut = dir+"/"+"sorted_"+FileNameInput;
        this.currDir = dir;
        if (isTest.length>0){
            this.isTest = isTest[0];
        }
        
    }
 
    public mergeSort(int maxMem,int poolSize, int keyIndexInput, String FileNameInput, Boolean... isTest){
        this.MAX_MEMORY = maxMem;
        if(Runtime.getRuntime().availableProcessors()>=poolSize){
            this.pool = Executors.newFixedThreadPool(poolSize);
        } else{
            System.err.println("the pool size is bigger than the number of available runtime processors so the number of threads will be 1");
            this.pool = Executors.newFixedThreadPool(1);
        }
        this.keyIndex = keyIndexInput;
        this.FileName = "/"+FileNameInput;
        this.FileNameOut = "sorted_"+FileNameInput;
        if (isTest.length>0){
            this.isTest = isTest[0];
        }        
    }


    public  void sortCSV() throws IOException {
        List<File> runs = splitCSV(FileName); 
        List<BufferedReader> results;
        
        try {
            results = readRuns(runs);
            
            BufferedWriter writer = new BufferedWriter(new FileWriter(FileNameOut));
            mergeRuns(results, writer);
            writer.close();

        } finally { 
            if( !isTest){
                cleanup(runs);
            }
            pool.shutdown();
        }
    }

    // creating a list of csv files with at most x lines
    private  List<File> splitCSV(String inputFile) throws IOException {
        
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        String header = reader.readLine();
        
        List<File> runs = new ArrayList<>();
        List<String> lines = new ArrayList<>();
        
        String tempFileFolderName = currDir+"/TempFilesDir";
        File tempFileFolder = new File(tempFileFolderName); 
        if (!tempFileFolder.exists()){
            tempFileFolder.mkdirs();
        }
        
        String line = reader.readLine();
        while (line != null) {
            
            lines.add(line);
            
            if (lines.size() >= MAX_MEMORY) {
                File file = writeRun(lines, header, tempFileFolder); 
                runs.add(file);
                lines.clear();
            }
            
            line = reader.readLine();
        }
        
        if (!lines.isEmpty()) {
            File file = writeRun(lines, header, tempFileFolder);
            runs.add(file);
        }
        reader.close();
        return runs;
    }
    
    // creating a CSV file with the lines given to it at the specified temp file folder
    private  File writeRun(List<String> lines, String header, File tempFileFolder) throws IOException {
        File file = File.createTempFile("run", ".csv",tempFileFolder); 
        
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
    


    // reding the files currently not parallelly
    private  List<BufferedReader> readRuns(List<File> runs) throws IOException {
    
        List<BufferedReader> results = new ArrayList<>();
        for(File file : runs) {
            try {
                results.add(new BufferedReader((new FileReader(file))));
            } catch(FileNotFoundException e) {}
        }
        return results;
    }


    // reading the files and sorting them while merge
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