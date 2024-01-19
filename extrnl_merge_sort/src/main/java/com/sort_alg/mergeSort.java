package com.sort_alg;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class mergeSort {

    private int MAX_MEMORY;
    private ExecutorService pool;
    private int keyIndex ;
    private String FileName;
    private String FileNameOut;
    private String currDir = System.getProperty("user.dir");
    private Boolean isTest = false;
    private String header;
    private File tempFileFolder;

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


    /**
     * @throws IOException
     */
    public  void sortCSV() throws IOException {

        String tempFileFolderName = currDir+"/TempFilesDir";
        tempFileFolder = new File(tempFileFolderName); 
        if (!tempFileFolder.exists()){
            tempFileFolder.mkdirs();
        }

        List<File> runs = splitCSV(FileName); 
        
        List<Future<File>> futures = new ArrayList<>();

        // List<BufferedReader> results;


    try{
        // for (int i = 0; i < runs.size(); i++) {
        //     final int index = i;
        //     Callable<String> sortTask = () -> {
        //     final List<CSVEntry> chunk = readRun(runs.get(index));
        //     chunk.sort(null);
        //     return writeRun(chunk, header,tempFileFolder);

        // for (int i = 0; i < runs.size(); i++) {
        //     final int index = i;
        for(final File file : runs) {
            final Callable<File> sortTask = new Callable<File>() {
                public File call() throws Exception {
                    final List<CSVEntry> chunk = readRun(file);
                    chunk.sort(null);
                    final List<String> sortedRun;
                    for (final CSVEntry oneCSVEntry: chunk){
                        sortedRun.add(oneCSVEntry.line);};
                    return writeRun(sortedRun,header);
                    }
            };
            futures.add(pool.submit(sortTask));
        }
            // results = readRuns(runs);
        
        List<File> sortedRuns = new ArrayList<>();

        for (Future<File> future : futures){
            sortedRuns.add(future.get());
        }


        mergeRuns()

    
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

         finally { 
                if( !isTest){
                    cleanup(runs);
                }
                pool.shutdown();
            }
        }


    // private String extracted(List<File> runs, final int index, File tempFileFolder) throws IOException {
        

    // creating a list of csv files with at most x lines
    private  List<File> splitCSV(String inputFile) throws IOException {
        
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        this.header = reader.readLine();
        
        List<File> runs = new ArrayList<>();
        List<String> lines = new ArrayList<>();

        String line = reader.readLine();
        while (line != null) {
            
            lines.add(line);
            
            if (lines.size() == MAX_MEMORY) {
                File file = writeRun(lines); 
                runs.add(file);
                lines.clear();
            }
            
            line = reader.readLine();
        }
        
        if (!lines.isEmpty()) {
            File file = writeRun(lines);
            runs.add(file);
        }
        reader.close();
        return runs;
    }
    
    // creating a CSV file with the lines given to it at the specified temp file folder
    private File writeRun(List<String> lines) throws IOException {
        
        String uid =  UUID.randomUUID().toString();
        File file = File.createTempFile("run_"+uid, ".csv",tempFileFolder); 
        
        writeRun(lines,file);

        
        return file;
    }
    
    private void writeRun(List<String> lines, File FileOut) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(FileOut));
        writer.write(header);
        writer.newLine();
        
        for(String line : lines) {
            writer.write(line);
            writer.newLine();
        }
        writer.close();
    }


    // reding the files currently not parallelly
    // private  List<BufferedReader> readRuns(List<File> runs) throws IOException {
    
    //     List<BufferedReader> results = new ArrayList<>();
    //     for(File file : runs) {
    //         try {
    //             results.add(new BufferedReader((new FileReader(file))));
    //         } catch(FileNotFoundException e) {}
    //     }
    //     return results;
    // }


    // read a single run with at most MAX_MEMORY lines
    private  List<CSVEntry> readRun(File run) throws IOException {
    
        List<CSVEntry> chunk = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(run))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                int key = Integer.parseInt(parts[keyIndex]); 
                chunk.add(new CSVEntry(key, line, null));
            }
        }
        return chunk;
    }


    // reading the files and sorting them while merge
    // private  void mergeRuns(List<BufferedReader> readers, Writer writer) throws IOException {
    // private  void mergeRuns(List<File> runs, String outputFile) throws IOException {

    //     PriorityQueue<CSVEntry> minHeap = new PriorityQueue<>();
        
    //     // for(BufferedReader reader : readers) {
    //     //     String line = reader.readLine();
    //     //     if(line != null) {
    //     //         String[] parts = line.split(",");
    //     //         int key = Integer.parseInt(parts[keyIndex]); 
    //     //         minHeap.add(new CSVEntry(key, line, reader));    
    //     //     }
    //     // }
        
    //     while(!minHeap.isEmpty()) {
           
    //         CSVEntry entry = minHeap.poll();
    //         writer.write(entry.line); 
    //         writer.write("\n");
    
    //         String nextLine = entry.reader.readLine();
    //         if(nextLine != null) {
    //             String[] parts = nextLine.split(",");
    //             int key = Integer.parseInt(parts[0]);
    //             minHeap.add(new CSVEntry(key, nextLine, entry.reader));
    //         }
    //     }
    // }
    


    private void mergeRuns(List<File> runs) throws IOException {
        PriorityQueue<CSVEntry> minHeap = new PriorityQueue<>();
    
        try {
            for (File run : runs) {
                BufferedReader reader = new BufferedReader(new FileReader(run));
                String line = reader.readLine();
                if(line != null && line !=header) {
                    String[] parts = line.split(",");
                    int key = Integer.parseInt(parts[keyIndex]); 
                    minHeap.add(new CSVEntry(key, line, reader));    
                }

    
                if (minHeap.size() == MAX_MEMORY) {
                    File tempChunkFile =  mergeRows(minHeap);
                    runs.add(tempChunkFile);
                }
            }

            mergeRows(minHeap,true);

            // Merge any remaining output chunks
            // mergeOutputChunks(outputChunks, writer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

    }


    private File mergeRows(PriorityQueue<CSVEntry> minHeap, Boolean... isFinelFile) throws IOException {
        File fileOut ;
        if (isFinelFile[0]==true){
            fileOut = File.createTempFile(FileNameOut, ".csv");
        }else{
            String uid =  UUID.randomUUID().toString();
            fileOut = File.createTempFile("run_"+uid, ".csv",tempFileFolder); 
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileOut));
        writer.write(header);
        writer.newLine();

        while (!minHeap.isEmpty()) {
            CSVEntry entry = minHeap.poll();
            
            writer.write(entry.line);
            writer.newLine();

            String nextLine = entry.reader.readLine();
            if(nextLine != null) {
                String[] parts = nextLine.split(",");
                int key = Integer.parseInt(parts[0]);
                minHeap.add(new CSVEntry(key, nextLine, entry.reader));
            }

        }
        writer.close();
        return fileOut;
    }
    

    // public void mergeRuns(List<BufferedReader> readers, Writer writer) throws IOException, InterruptedException {

    //     Semaphore semaphore = new Semaphore(MAX_MEMORY / ESTIMATED_READER_MEMORY); 
        
    //     ExecutorService threadPool = Executors.newFixedThreadPool(RUNS_PER_THREAD);
    
    //     for(int i=0; i< readers.size(); i+= RUNS_PER_THREAD) {
    
    //         List<BufferedReader> group = readers.subList(i, Math.min(i+RUNS_PER_THREAD, readers.size());
    
    //         threadPool.execute(() -> {
    
    //             PriorityQueue<CSVEntry> minHeap = new PriorityQueue<>(group.size());
    
    //             for(BufferedReader reader : group) { 
    //                 semaphore.acquire();
    //                 // read entry & add to min heap 
    //                 semaphore.release();
    //             }
    
    //             while(!minHeap.isEmpty()) {
    //                 CSVEntry entry = minHeap.poll();
    //                 writer.write(entry.line);
    //                 writer.newLine();
    
    //                 semaphore.acquire();
    //                 String nextLine = entry.reader.readLine(); 
    //                 if(nextLine != null) {
    //                     // parse and add new entry to heap
    //                 }
    //                 semaphore.release();
    //             }
    
    //         });
    //     }
    
    //     threadPool.shutdown();
    //     threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        
    // }


    private  void cleanup(List<File> runs) {
        for(File file : runs) {
            file.delete(); 
        }
    }

}