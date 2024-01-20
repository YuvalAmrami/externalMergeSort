package com.sort_alg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import org.junit.Before;
import org.junit.Test;

public class mergeSortTest {
    Path resourceDirectory;

    @Before
    public void init() {
        resourceDirectory = Paths.get("src", "test", "resources");
    }
 
    @Test
    public void testSplitCSV1() throws IOException, NoSuchMethodException, SecurityException, IllegalAccessException,
            InvocationTargetException {

        MergeSort mergeSort = new MergeSort(100, 2, 0, "sample1.csv",resourceDirectory.toString());

        Method splitCSV = MergeSort.class.getDeclaredMethod("splitCSV", String.class);
        splitCSV.setAccessible(true);
        Method cleanup = MergeSort.class.getDeclaredMethod("cleanup",List.class);
        cleanup.setAccessible(true);

        // Call the splitCSV function, passing in your sample file
        Object invoke = splitCSV.invoke(mergeSort, resourceDirectory + "/sample1.csv");
        List<File> runs = (List<File>) invoke;

        // Assert that it returned the expected number of runs
        assertEquals(1, runs.size());

        // Open the run file and check it contains the expected rows
        File runFile = runs.get(0);
        BufferedReader reader = new BufferedReader(new FileReader(runFile));

        String line = reader.readLine(); // header
        assertEquals("header1,header2,header3", line);

        line = reader.readLine(); // row 1
        assertEquals("row1col1,row1col2,row1col3", line);

        line = reader.readLine(); // row 2
        assertEquals("row2col1,row2col2,row2col3", line);

        reader.close();
        cleanup.invoke(mergeSort, runs);

    }

    @Test
    public void testSplitCSV2() throws IOException, NoSuchMethodException, SecurityException, IllegalAccessException,
            InvocationTargetException {

        MergeSort mergeSort = new MergeSort(2, 2, 0, "sample2.csv",resourceDirectory.toString());

        Method splitCSV = MergeSort.class.getDeclaredMethod("splitCSV", String.class);
        splitCSV.setAccessible(true);
        Method cleanup = MergeSort.class.getDeclaredMethod("cleanup",List.class);
        cleanup.setAccessible(true);


        // Call the splitCSV function, passing in your sample file
        List<File> runs = (List<File>) splitCSV.invoke(mergeSort, resourceDirectory + "/sample2.csv");

        // Assert that it returned the expected number of runs
        assertEquals(3, runs.size());

        // Open the run file and check it contains the expected rows
        File runFile = runs.get(0);
        BufferedReader reader = new BufferedReader(new FileReader(runFile));

        String line = reader.readLine(); // header
        assertEquals("header1,header2,header3", line);

        line = reader.readLine(); // row 1
        assertEquals("row1col1,row1col2,row1col3", line);

        line = reader.readLine(); // row 2
        assertEquals("row2col1,row2col2,row2col3", line);
        
        reader.close();

        runFile = runs.get(2);
        reader = new BufferedReader(new FileReader(runFile));

        line = reader.readLine(); // header
        assertEquals("header1,header2,header3", line);

        line = reader.readLine(); // row 1
        assertEquals("row5col1,row5col2,row5col3", line);

        line = reader.readLine(); // row 2
        assertEquals("row6col1,row6col2,row6col3", line);

        reader.close();
        cleanup.invoke(mergeSort, runs);

        
    }

    @Test
    public void readRunAndSortTest() throws NoSuchMethodException, SecurityException, IllegalAccessException, InvocationTargetException {
        MergeSort mergeSort = new MergeSort(10, 2, 0, "sample3.csv",resourceDirectory.toString());

        // methods:
        Method splitCSV = MergeSort.class.getDeclaredMethod("splitCSV", String.class);
        Method readRun = MergeSort.class.getDeclaredMethod("readRun", File.class);
        Method cleanup = MergeSort.class.getDeclaredMethod("cleanup",List.class);

        splitCSV.setAccessible(true);
        readRun.setAccessible(true);
        cleanup.setAccessible(true);


        // Call the splitCSV function, passing in your sample file
        List<File> runs = (List<File>) splitCSV.invoke(mergeSort, resourceDirectory + "/sample3.csv");

        assertEquals(1, runs.size());

        // Open the run file and check it contains the expected rows
        File runFile = runs.get(0);

        List<CSVEntry> CSVRun  = (List<CSVEntry>) readRun.invoke(mergeSort, runFile);
        
        
        assertEquals(CSVRun.get(0).key, new CSVEntry(2, "2,row1col2,row1col3", null).key);
        assertEquals(CSVRun.get(0).line, new CSVEntry(2, "2,row1col2,row1col3", null).line);

        assertEquals(CSVRun.get(1).key, new CSVEntry(1, "1,row2col2,row2col3", null).key);
        assertEquals(CSVRun.get(1).line, new CSVEntry(1, "1,row2col2,row2col3", null).line);

        assertEquals(CSVRun.get(5).key, new CSVEntry(4, "4,row6col2,row6col3", null).key);
        assertEquals(CSVRun.get(5).line, new CSVEntry(4, "4,row6col2,row6col3", null).line);

 
        CSVRun.sort(null);

        assertEquals(CSVRun.get(0).key, new CSVEntry(1, "1,row2col2,row2col3", null).key);
        assertEquals(CSVRun.get(0).line, new CSVEntry(1, "1,row2col2,row2col3", null).line);

        assertEquals(CSVRun.get(1).key, new CSVEntry(2, "2,row1col2,row1col3", null).key);
        assertEquals(CSVRun.get(1).line, new CSVEntry(2, "2,row1col2,row1col3", null).line);

        assertEquals(CSVRun.get(5).key, new CSVEntry(7, "7,row4col2,row4col3", null).key);
        assertEquals(CSVRun.get(5).line, new CSVEntry(7, "7,row4col2,row4col3", null).line);


        cleanup.invoke(mergeSort, runs);

    }
  
    @Test
    public void writeRunTest() throws NoSuchMethodException, SecurityException, IllegalAccessException, InvocationTargetException, IOException {
        MergeSort mergeSort = new MergeSort(10, 2, 0, "sample1.csv",resourceDirectory.toString());

        // methods:
        Method writeRun = MergeSort.class.getDeclaredMethod("writeRun", List.class);
        writeRun.setAccessible(true);


        List<String> lines = new ArrayList<>();
        lines.add("2,row1col2,row1col3");
        lines.add("1,row2col2,row2col3");
        lines.add("4,row6col2,row6col3");

        File written  = (File) writeRun.invoke(mergeSort, lines);

        BufferedReader reader = new BufferedReader(new FileReader(written));

        String line = reader.readLine(); // row 1
        assertEquals("2,row1col2,row1col3", line);

        line = reader.readLine(); // row 1
        assertEquals("1,row2col2,row2col3", line);

        line = reader.readLine(); // row 2
        assertEquals("4,row6col2,row6col3", line);
 
        reader.close();
        written.delete();

    }


    @Test
    public void testMergeMinHeap() throws NoSuchMethodException, SecurityException, IOException, IllegalAccessException, InvocationTargetException{
        MergeSort mergeSort = new MergeSort(2, 2, 0, "sample4.csv",resourceDirectory.toString());

        String header = "header1,header2,header3";
        int keyIndex = 0;

        // methods:
        Method splitCSV = MergeSort.class.getDeclaredMethod("splitCSV", String.class);
        splitCSV.setAccessible(true);
        Method mergeMinHeap = MergeSort.class.getDeclaredMethod("mergeMinHeap", PriorityQueue.class,Boolean.class);
        mergeMinHeap.setAccessible(true);


        // keeping in mind the rounds themselves were not ordered yet sample4 is designed to be in ordered
        List<File> runs = (List<File>) splitCSV.invoke(mergeSort, resourceDirectory + "/sample4.csv");
        List<String> FileNames = new ArrayList<>();

        PriorityQueue<CSVEntry> minHeap = new PriorityQueue<>();

        for (File run : runs) {
            run.deleteOnExit(); 
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

        File merged = (File) mergeMinHeap.invoke(mergeSort, minHeap,false);

        BufferedReader reader = new BufferedReader(new FileReader(merged));

        String line = reader.readLine(); // header
        assertEquals(header, line);

        line = reader.readLine(); // row 1
        assertEquals("1,row1col2,row1col3", line);
        line = reader.readLine(); // row 2
        assertEquals("2,row2col2,row2col3", line);
        line = reader.readLine(); // row 3
        assertEquals("3,row3col2,row3col3", line);
        line = reader.readLine(); // row 4
        assertEquals("4,row5col2,row5col3", line);
        line = reader.readLine(); // row 5
        assertEquals("5,row6col2,row6col3", line);
        line = reader.readLine(); // row 5
        assertEquals("7,row4col2,row4col3", line);
        
        reader.close();
        merged.delete();
        
    }


    // mergeRunsParallel

    // mergeRunsOneThread /last & no last

    // mergeRuns


    // mergeMinHeap


    @Test
    public void fullExternalMergeSort(){

        // sortCSV() function
        assertTrue( true );
    }

}
