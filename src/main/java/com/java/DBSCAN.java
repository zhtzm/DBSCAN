package com.java;

import com.java.ds.CType;
import com.java.ds.Point;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class DBSCAN {
    private static int ClusterId = 0;
    private final List<Point> Data;

    public DBSCAN(List<Point> data) {
        Data = data;
    }

    private double distance(Point p, Point q){
        if(p.getSize() != q.getSize()){
            return Double.POSITIVE_INFINITY;
        } else {
            double sum=0.0;
            int len = p.getSize();
            for(int i = 0; i < len; i++) {
                sum += (p.getElements().get(i) - q.getElements().get(i))
                        * (p.getElements().get(i) - q.getElements().get(i));
            }
            return Math.sqrt(sum);
        }
    }

    private List<Point> GetNeighborhood(Point p, double epi){
        List<Point> vp = new ArrayList<>();
        for (Point q : Data) {
            if (distance(p, q) <= epi) {
                vp.add(q);
            }
        }
        return vp;
    }

    public void RunDBSCAN(int minPts, double epsilon) {
        for (Point p : Data) {
            if(!p.isVisited()) {
                p.setVisited(true);
                List<Point> neighbors = this.GetNeighborhood(p, epsilon);
                if(neighbors.size() < minPts) {
                    p.setCt(CType.NOISE);
                } else {
                    expandCluster(p, neighbors, minPts, epsilon, ClusterId);
                    ++ClusterId;
                }
            }
        }
    }

    private void expandCluster(Point p, List<Point> neighbors, int minPts, double epsilon, int ClusterId) {
        p.setClusterId(ClusterId);
        p.setCt(CType.CORE);
        LinkedList<Point> queue = new LinkedList<>(neighbors);

        while(!queue.isEmpty()) {
            Point q = queue.poll();
            if(!q.isVisited()) {
                q.setVisited(true);
                q.setClusterId(ClusterId);
                List<Point> qNeighbors = GetNeighborhood(q, epsilon);
                if(qNeighbors.size() >= minPts) {
                    q.setCt(CType.CORE);
                    queue.addAll(qNeighbors);
                } else {
                    q.setCt(CType.BORDER);
                }
            } else if(q.getCt() == CType.NOISE) {
                q.setClusterId(ClusterId);
                q.setCt(CType.BORDER);
            }
        }
    }
}
