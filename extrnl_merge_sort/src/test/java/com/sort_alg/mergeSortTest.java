package com.sort_alg;

import static org.junit.Assert.assertEquals;

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
        // InputStream is =
        // getClass().getResourceAsStream("/test-resources/sample1.csv");

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
    }

    @Test
    public void testSplitCSV2() throws IOException, NoSuchMethodException, SecurityException, IllegalAccessException,
            InvocationTargetException {

        MergeSort mergeSort = new MergeSort(2, 2, 0, "sample2.csv",resourceDirectory.toString());

        Method splitCSV = MergeSort.class.getDeclaredMethod("splitCSV", String.class);
        splitCSV.setAccessible(true);

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
        MergeSort mergeSort = new MergeSort(10, 2, 0, "sample1.csv");

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


}
