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


    public int equals(CSVEntry other){
        if (this == other) return 0;
        if (other == null) return 1;
        if (getClass() != other.getClass()) return 1;
        if (key != other.key) return 1;
        if (line != other.line) return 1;
        if (reader != other.reader) return 1;
        return 0;
    }

    public int assertEquals(CSVEntry other){
        if (this == other) return 0;
        if (other == null) return 1;
        if (getClass() != other.getClass()) return 1;
        if (key != other.key) return 1;
        if (line != other.line) return 1;
        if (reader != other.reader) return 1;
        return 0;
    }
    
}

