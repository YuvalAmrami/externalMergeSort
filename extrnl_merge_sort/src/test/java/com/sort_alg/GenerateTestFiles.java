package com.sort_alg;

import java.io.BufferedWriter; 
import java.io.FileWriter;
import java.util.Random;

public class GenerateTestFiles {

    private static final int NUM_FILES = 5; 
    private static final int NUM_ROWS = 50;
    private static final int NUM_COLUMNS = 5;
    
    public static void main(String[] args) throws Exception {
     
        Random rand = new Random();
        
        for(int f=1; f<=NUM_FILES; f++) {
            
            BufferedWriter writer = new BufferedWriter(new FileWriter("test" + f + ".csv"));
            
            writer.write("id,col1,col2,col3,col4\n");
            
            for(int r=1; r<=NUM_ROWS; r++) {  
                int id = rand.nextInt(1000000);
                String lineText = String.valueOf(id);
                for(int colNum=1; colNum<NUM_COLUMNS; colNum++) {
                    int col= rand.nextInt(1000000); 
                    lineText = lineText + ","+col;
                }
                lineText = lineText + "\n";                
                writer.write(lineText);
            }
            
            writer.close();
        }
        
        System.out.println("Generated " + NUM_FILES +" test CSV files");
        
    }

}