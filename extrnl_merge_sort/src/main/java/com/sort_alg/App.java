package com.sort_alg;

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
        String nameOfCSVFile = "a.csv";

        for(int i = 0; i<(args.length-1);i++){
            String arg = args[i];
            if (arg.contains("-")){
                switch(arg.toLowerCase()) {
                    case "-k":
                        keyVal = Integer.valueOf(args[i+1]);
                    case "-x":
                        sizeOfMemory = Integer.valueOf(args[i+1]);
                    case "-t":
                        numberOfThreads = Integer.valueOf(args[i+1]);
                    case "-f":
                    nameOfCSVFile = args[i+1];
                }

            }
        } 

        System.out.println( "Hello World!" );
        System.out.print( "keyVal: "+keyVal );
        System.out.print( "sizeOfMemory: "+sizeOfMemory );
        System.out.print( "numberOfThreads: "+numberOfThreads );
        System.out.print( "nameOfCSVFile: "+nameOfCSVFile );

        System.out.println( "Hello World!" );
    }
}