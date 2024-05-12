package com.java;

import com.java.MR.LocalClustering;

import java.io.IOException;

public class TestLC {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        LocalClustering.clustering(30, 20000, "10000data");
    }
}
