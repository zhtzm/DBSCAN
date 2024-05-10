package com.java;

import com.java.ds.CType;
import com.java.ds.Point;

import java.util.ArrayList;
import java.util.List;

public class DBSCAN {
    private int ClusterId;
    private final List<Point> Data;

    public DBSCAN(List<Point> data) {
        Data = data;
        ClusterId = 0;
    }

    public double distance(Point p, Point q){
        double dis;
        if(p.getSize() == q.getSize()){
            double sum=0.0;
            int len= p.getSize();
            for(int i=0;i<len;i++) {
                sum += (p.getElements().get(i) - q.getElements().get(i)) * (p.getElements().get(i) - q.getElements().get(i));
            }
            return Math.sqrt(sum);
        }
        return Double.POSITIVE_INFINITY;
    }

    public List<Point> GetNeighborhood(Point p, double epi){
        int sum=0;
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
            if (!p.isVisited()) {
                p.setVisited(true);
                List<Point> neighbors = GetNeighborhood(p, epsilon);
                if (neighbors.size() < minPts) {
                    p.setCt(CType.NOISE);
                } else {
                    p.setClusterId(ClusterId);
                    p.setCt(CType.CORE);
                    for (int j = 0; j < neighbors.size(); j++) {
                        Point p1 = neighbors.get(j);
                        if (!p1.isVisited()) {
                            p1.setVisited(true);
                            p1.setClusterId(ClusterId);

                            List<Point> neighbors1 = GetNeighborhood(p1, epsilon);
                            if (neighbors1.size() >= minPts) {
                                p1.setCt(CType.CORE);
                                for (Point sk : neighbors1) {
                                    if (!neighbors.contains(sk)) {
                                        neighbors.add(sk);
                                    }
                                }
                            } else {
                                p1.setCt(CType.BORDER);
                            }
                        } else if (p1.getCt() == CType.NOISE) {
                            p1.setClusterId(ClusterId);
                            p1.setCt(CType.BORDER);
                        }
                    }
                }
            }
            this.ClusterId++;
        }
    }
}
