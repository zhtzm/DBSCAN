package com.java.ds;

import java.util.ArrayList;

public class Point {
    private long id;
    private int size;
    private final ArrayList<Double> elements;
    private final PType pt;

    private int clusterId;
    private int PartitionId;
    private CType ct;
    private boolean visited;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getPartitionId() {
        return PartitionId;
    }

    public PType getPt() {
        return pt;
    }

    public Point(String str, Integer partition) {
        String[] parts = str.split(" ");
        id = Long.parseLong(parts[0]);
        switch (parts[1]) {
            case "IR":
                this.pt = PType.IR;
                break;
            case "IM":
                this.pt = PType.IM;
                break;
            case "OM":
                this.pt = PType.OM;
                break;
            default:
                this.pt = PType.NA;
        }

        this.size = 0;
        this.elements = new ArrayList<>();
        for (int i = 2; i < parts.length; i++) {
            elements.add(Double.parseDouble(parts[i]));
            this.size++;
        }

        clusterId = -1;
        visited = false;
        PartitionId = partition;
    }

    public String getElementsString() {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < elements.size(); i++) {
            if (i == elements.size()-1) {
                str.append(elements.get(i));
            } else {
                str.append(elements.get(i)).append(",");
            }
        }
        return str.toString();
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public boolean isVisited() {
        return visited;
    }

    public void setVisited(boolean visited) {
        this.visited = visited;
    }

    public ArrayList<Double> getElements() {
        return elements;
    }

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public void setPartitionId(int partitionId) {
        this.PartitionId = partitionId;
    }

    public PType getPType() {
        return pt;
    }

    public CType getCt() {
        return ct;
    }

    public void setCt(CType ct) {
        this.ct = ct;
    }
}
