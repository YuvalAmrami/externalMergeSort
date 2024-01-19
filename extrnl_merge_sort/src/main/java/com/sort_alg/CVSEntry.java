package com.sort_alg;

import java.io.BufferedReader;

class CSVEntry implements Comparable<CSVEntry> {

    int key;
    String line;
    BufferedReader reader;
    
    public CSVEntry(int key, String line, BufferedReader reader) {
        this.key = key;
        this.line = line;
        this.reader = reader;
    }
    
    @Override
    public int compareTo(CSVEntry other){
        return Integer.compare(key, other.key); 
    }

    
}

