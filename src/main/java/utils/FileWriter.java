package main.java.utils;

import java.io.BufferedWriter;
import java.io.IOException;

public class FileWriter {

    public void writeResult(String filename, String result) throws IOException {

        BufferedWriter writer = new BufferedWriter(new java.io.FileWriter(filename, true));
        writer.newLine();
        writer.append(result);

        writer.close();
    }
}
