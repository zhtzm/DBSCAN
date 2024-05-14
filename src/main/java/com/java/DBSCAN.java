package com.java;

import com.java.ds.CType;
import com.java.ds.Point;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DBSCAN {
    private static final AtomicInteger clusterIdCounter = new AtomicInteger(0);
    private final List<Point> data;

    public DBSCAN(List<Point> data) {
        this.data = data;
    }

    private double distance(Point p, Point q) {
        if (p.getSize() != q.getSize()) {
            return Double.POSITIVE_INFINITY;
        } else {
            double sum = 0.0;
            int len = p.getSize();
            for (int i = 0; i < len; i++) {
                sum += Math.pow(p.getElements().get(i) - q.getElements().get(i), 2);
            }
            return Math.sqrt(sum);
        }
    }

    private List<Point> getNeighborhood(Point p, double epsilon) {
        List<Point> neighbors = new ArrayList<>();
        for (Point q : data) {
            if (distance(p, q) <= epsilon) {
                neighbors.add(q);
            }
        }
        return neighbors;
    }

    public void RunDBSCAN(int minPts, double epsilon) {
        for (Point p : data) {
            if (!p.isVisited()) {
                p.setVisited(true);
                List<Point> neighbors = getNeighborhood(p, epsilon);
                if (neighbors.size() < minPts) {
                    p.setCt(CType.NOISE);
                } else {
                    int clusterId = clusterIdCounter.getAndIncrement();
                    expandCluster(p, neighbors, minPts, epsilon, clusterId);
                }
            }
        }
    }

    private void expandCluster(Point p, List<Point> neighbors, int minPts, double epsilon, int clusterId) {
        p.setClusterId(clusterId);
        p.setCt(CType.CORE);
        LinkedList<Point> queue = new LinkedList<>(neighbors);

        while (!queue.isEmpty()) {
            Point q = queue.poll();
            if (!q.isVisited()) {
                q.setVisited(true);
                q.setClusterId(clusterId);
                List<Point> qNeighbors = getNeighborhood(q, epsilon);
                if (qNeighbors.size() >= minPts) {
                    q.setCt(CType.CORE);
                    queue.addAll(qNeighbors);
                } else {
                    q.setCt(CType.BORDER);
                }
            } else if (q.getCt() == CType.NOISE) {
                q.setClusterId(clusterId);
                q.setCt(CType.BORDER);
            }
        }
    }
}
