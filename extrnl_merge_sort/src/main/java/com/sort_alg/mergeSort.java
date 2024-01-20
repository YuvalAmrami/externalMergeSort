package com.sort_alg;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class MergeSort {

    // class variables
    private int MAX_MEMORY;
    private ExecutorService pool;
    private int keyIndex;
    private String FileName;
    private String FileNameOut;
    private String currDir = System.getProperty("user.dir");
    private Boolean isTest = false;

    // recurring calls
    private File tempFileFolder;
    private int Uid;
    private String header;


    //constructors and initializers
    public MergeSort(int maxMem, int poolSize, int keyIndexInput, String FileNameInput, String dir, Boolean... isTest) {
        
        this.MAX_MEMORY = maxMem;
        if (Runtime.getRuntime().availableProcessors() >= poolSize) {
            this.pool = Executors.newFixedThreadPool(poolSize);
        } else {
            System.err.println(
                    "the pool size is bigger than the number of available runtime processors so the number of threads will be 1");
            this.pool = Executors.newFixedThreadPool(1);
        }
        this.keyIndex = keyIndexInput;
        this.FileName = dir + "/" + FileNameInput;
        this.FileNameOut = dir + "/" + "sorted_" + FileNameInput;
        this.currDir = dir;
        if (isTest.length > 0) {
            this.isTest = isTest[0];
        }

        String tempFileFolderName = currDir + "/TempFilesDir";
        tempFileFolder = new File(tempFileFolderName);
        if (!tempFileFolder.exists()) {
            if (!tempFileFolder.mkdirs()) {
                System.err.println("Error creating directories for " + (tempFileFolder).toString());
            }
        }
        Uid = 1;
    }

    public MergeSort(int maxMem, int poolSize, int keyIndexInput, String FileNameInput, Boolean... isTest) {
        
        this.MAX_MEMORY = maxMem;
        if (Runtime.getRuntime().availableProcessors() >= poolSize) {
            this.pool = Executors.newFixedThreadPool(poolSize);
        } else {
            System.err.println(
                    "the pool size is bigger than the number of available runtime processors so the number of threads will be 1");
            this.pool = Executors.newFixedThreadPool(1);
        }
        this.keyIndex = keyIndexInput;
        this.FileName = "/" + FileNameInput;
        this.FileNameOut = "sorted_" + FileNameInput;
        if (isTest.length > 0) {
            this.isTest = isTest[0];
        }

        String tempFileFolderName = currDir + "/TempFilesDir";
        tempFileFolder = new File(tempFileFolderName);
        if (!tempFileFolder.exists()) {
            if (!tempFileFolder.mkdirs()) {
                System.err.println("Error creating directories for " + (tempFileFolder).toString());
            }
        }
        Uid = 1;
    }

    public void sortCSV() throws IOException {

        List<File> runs = splitCSV(FileName);

        List<Future<File>> futures = new ArrayList<>();
        try {
            for (final File file : runs) {
                final Callable<File> sortTask = new Callable<File>() {
                    public File call() throws Exception {
                        final List<CSVEntry> run = readRun(file);
                        run.sort(null);
                        List<String> sortedRun = new ArrayList<>();
                        for (final CSVEntry oneCSVEntry : run) {
                            sortedRun.add(oneCSVEntry.line);
                        };
                        return writeRun(sortedRun);
                    }
                };
                futures.add(pool.submit(sortTask));
            }

            List<File> sortedRuns = new ArrayList<>();
            for (Future<File> future : futures) {
                sortedRuns.add(future.get());
            }

            mergeRunsParallel(sortedRuns);

        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        finally {
            // cleanup(runs);
            pool.shutdown();
        }
    }

    // creating a list of csv files with at most MAX_MEMORY lines
    private List<File> splitCSV(String inputFile) throws IOException {

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

    // read a single run with at most MAX_MEMORY lines to be sorted in memory
    private List<CSVEntry> readRun(File run) throws IOException {

        List<CSVEntry> chunk = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(run))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (header!=null && line.equals(header))
                    continue;
                String[] parts = line.split(",");
                int key = Integer.parseInt(parts[keyIndex]);
                chunk.add(new CSVEntry(key, line, null));
            }
        }
        return chunk;
    }

    // creating a CSV file with the lines given to it at the universal specified
    // temp file folder
    private File writeRun(List<String> lines) throws IOException {

        String uid = getUid();
        File file = File.createTempFile("run_" + uid, ".csv", tempFileFolder);

		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(file));
            if(header!=null){
                writer.write(header);
                writer.newLine();
            }

            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
            writer.close();

            return file;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // running through the ordered files generated and being generated and merging
    // them a new file ordered using the thread pool
    private void mergeRunsParallel(List<File> runs) throws IOException, InterruptedException, ExecutionException {

        if (runs.size() > MAX_MEMORY) {

            List<List<File>> runBatches = new ArrayList<>();
            // chunks of MAX_MEMORY runs
            for (int i = 0; i < runs.size(); i += MAX_MEMORY) {
                int end = Math.min(runs.size(), i + MAX_MEMORY);
                runBatches.add(runs.subList(i, end));
            }
            try {
                List<Future<File>> mergeFutures = new ArrayList<>();

                for (final List<File> runBatch : runBatches) {
                    final Callable<File> mergeTask = new Callable<File>() {
                        public File call() throws Exception {
                            return mergeRunsOneThread(runBatch, false);
                        }
                    };
                    mergeFutures.add(pool.submit(mergeTask));
                }

                List<File> mergedRuns = new ArrayList<>();

                for (Future<File> future : mergeFutures) {
                    mergedRuns.add(future.get());
                }

                mergeRunsParallel(mergedRuns);

            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } catch (InterruptedException ie) {
                System.err.println("Thread interrupted. "+ ie);
                throw new InterruptedException(ie.getMessage());
            }
        } else {
            mergeRunsOneThread(runs,true);
        }
    }


    // merging for a single thread: max runs are MAX_MEMORY
    private File mergeRunsOneThread(List<File> runs, Boolean isLast) throws IOException {
        PriorityQueue<CSVEntry> minHeap = new PriorityQueue<>();

        try {
            for (File run : runs) {
                BufferedReader reader = new BufferedReader(new FileReader(run));
                String line = reader.readLine();
                if (header!=null && line.equals(header)) {
                    line = reader.readLine();
                }
                if (line != null) {
                    String[] parts = line.split(",");
                    int key = Integer.parseInt(parts[keyIndex]);
                    minHeap.add(new CSVEntry(key, line, reader));
                }
            }
            File merged = mergeMinHeap(minHeap,isLast);
            cleanup(runs);
            return merged;

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

    }

    
    // merging the files from a minimum Heap of at most MAX_MEMORY entries at a time
    private File mergeMinHeap(PriorityQueue<CSVEntry> minHeap, Boolean isFinelFile) throws IOException {
        File fileOut;
        if (isFinelFile == true) {
            fileOut = File.createTempFile(FileNameOut, ".csv");
        } else {
            String uid = getUid();
            fileOut = File.createTempFile("run_" + uid, ".csv", tempFileFolder);
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileOut));
        if (header != null){
            writer.write(header);
            writer.newLine();
        }
        while (!minHeap.isEmpty()) {
            CSVEntry entry = minHeap.poll();

            writer.write(entry.line);
            writer.newLine();

            String nextLine = entry.reader.readLine();
            if (nextLine != null) {
                String[] parts = nextLine.split(",");
                int key = Integer.parseInt(parts[0]);
                minHeap.add(new CSVEntry(key, nextLine, entry.reader));
            }else{
                entry.reader.close();
            }

        }
        writer.close();
        return fileOut;
    }

    private void cleanup(List<File> runs) {
        if (!isTest) {
            for (File file : runs) {
                file.delete();
            }
        }
    }

    private String getUid() {
        int tempUdi = Uid;
        Uid = Uid + 1;
        return String.valueOf(tempUdi);
    }

}