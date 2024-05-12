package com.java;

import com.java.mapper.Hive;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.java.MR_DBSCAN.hive;

public class Draw {
    private static final int SATURATION = 200;
    private static final int BRIGHTNESS = 200;

    public static void draw(Integer num_field, String output) throws SQLException, ClassNotFoundException, IOException {
        Double[] minArray = new Double[num_field];
        Double[] maxArray = new Double[num_field];
        for (int i = 0; i < num_field; i++) {
            try (ResultSet res = hive.sql(null,
                    "SELECT MIN(x" + i + ") AS min_value FROM final_merge")) {
                if (res.next()) {
                    minArray[i] = Double.parseDouble(res.getString("min_value"));
                }
            }
            Hive.destroy();
            try (ResultSet res = hive.sql(null,
                    "SELECT MAX(x" + i + ") AS max_value FROM final_merge")) {
                if (res.next()) {
                    maxArray[i] = Double.parseDouble(res.getString("max_value"));
                }
            }
            Hive.destroy();
        }

        List<Integer> num_cluster = new ArrayList<>();
        try (ResultSet res = hive.sql(null, "SELECT DISTINCT cid2 FROM final_merge")) {
            while (res.next()) {
                int cid2 = Integer.parseInt(res.getString(1));
                if (cid2 != -1)
                    num_cluster.add(cid2);
            }
        }
        Map<Integer, Color> color_map = allocateColors(num_cluster);

        double minX = minArray[0];
        double maxX = maxArray[0];
        double minY = minArray[1];
        double maxY = maxArray[1];

        int imageWidth = 1600;
        int imageHeight = 900;

        BufferedImage image = new BufferedImage(imageWidth, imageHeight, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2d = image.createGraphics();

        double scaleX = (double) imageWidth / (maxX - minX);
        double scaleY = (double) imageHeight / (maxY - minY);

        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "开始绘点");
        try (ResultSet res = hive.sql(null, "select * from final_merge")) {
            while (res.next()) {
                int cid2 = Integer.parseInt(res.getString("cid2"));
                String ct = res.getString("ct");
                double x1 = Double.parseDouble(res.getString("x0"));
                double x2 = Double.parseDouble(res.getString("x1"));

                int px = (int) ((x1 - minX) * scaleX);
                int py = imageHeight - (int) ((x2 - minY) * scaleY); // 注意Y轴是反的

                Color color = color_map.get(cid2);
                g2d.setColor(color);
                g2d.fillOval(px, py, 2, 2); // 绘制一个5x5的圆点
            }
        }
        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "绘点完成");

        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "开始绘图");
        ImageIO.write(image, "png", new File(output + "/output.png"));
        g2d.dispose();
        System.out.println(MR_DBSCAN.Formatter.format(LocalDateTime.now()) + "绘图完成");
    }

    public static Map<Integer, Color> allocateColors(List<Integer> num_cluster) {
        Map<Integer, Color> colorMap = new HashMap<>();

        int all = num_cluster.size();
        for (int i = 0; i < all; i++) {
            int cid = num_cluster.get(i);
            double hue = (double) (i + 1) / (all + 1) * 360;
            Color color = Color.getHSBColor((float) (hue / 360), SATURATION / 255f, BRIGHTNESS / 255f);
            colorMap.put(cid, color);
        }

        colorMap.put(-1, Color.BLACK);
        return colorMap;
    }
}
