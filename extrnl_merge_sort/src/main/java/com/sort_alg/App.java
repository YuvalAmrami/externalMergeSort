package com.sort_alg;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        int keyVal = 0;
        int sizeOfMemory = 2;
        int numberOfThreads = 1;
        String currDir = System.getProperty("user.dir");
        String resourceDirectory = currDir +"\\"+ Paths.get("extrnl_merge_sort","src", "main","java","com","sort_alg","resources").toString();
        String nameOfCSVFile = "a.csv";
        Boolean isTest = false;

        for(int i = 0; i<(args.length-1);i++){
            String arg = args[i];
            if (arg.contains("-")){
                switch(arg.toLowerCase()) {
                    case "-k":  //key
                        keyVal = Integer.valueOf(args[i+1]);
                        System.out.println( "input key value is: "+ keyVal );
                        break;
                    case "-x":  //x lines active
                        sizeOfMemory = Integer.valueOf(args[i+1]);
                        System.out.println( "input max number of lines is: "+ sizeOfMemory);
                        break;
                    case "-t":  //thread number
                        numberOfThreads = Integer.valueOf(args[i+1]);
                        System.out.println( "input number of threads: "+ numberOfThreads);
                        break;
                    case "-f":  //file name
                        nameOfCSVFile = args[i+1];
                        System.out.println( "input name of CSV file: "+nameOfCSVFile);
                        break;
                    case "-p":  //path
                        resourceDirectory = args[i+1];
                        System.out.println( "input dir: "+ resourceDirectory);
                        break;
                    case "-d":  //debug?
                        isTest = true;
                        System.out.println( "the app is working on debug mode and will not delete the temporary files");
                        break;


                }

            }
        }
        if (args.length!=0 && args[args.length-1].toLowerCase().equals("-d")){
            isTest = true;
            System.out.println( "the app is working on debug mode and will not delete the temporary files");
        }

        MergeSort mergeSort = new MergeSort(sizeOfMemory, numberOfThreads, keyVal, nameOfCSVFile, resourceDirectory.toString(),isTest);
        try {
            mergeSort.sortCSV();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
