package com.java.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFS {
    private static final String URL = "hdfs://192.168.64.131:9000";
    public static final String JOB_PATH = "/job/DBSCAN";
    private static FileSystem fs = null;

    public void init() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS",URL);
        System.setProperty("HADOOP_USER_NAME","root");
        fs = FileSystem.get(conf);
    }

    public void close() throws IOException {
        if (fs != null)
            fs.close();
    }

    public void upload(String sourceFile, String targetPath) throws IOException {
        Path src = new Path(sourceFile);
        Path tgt = new Path(targetPath);
        fs.copyFromLocalFile(src,tgt);
        System.out.println("上传成功");
    }

    public void download(String sourceFile, String targetPath) throws IOException {
        Path src = new Path(sourceFile);
        Path tgt = new Path(targetPath);
        fs.copyToLocalFile(false,src,tgt,true);
        System.out.println("下载成功");
    }

    public void mkdir(String path) throws IOException {
        Path src = new Path(path);
        fs.mkdirs(src);
        System.out.println("创建成功");
    }

    public void rename(String oldName, String newName) throws IOException {
        Path src = new Path(oldName);
        Path tgt = new Path(newName);
        fs.rename(src,tgt);
        System.out.println("重命名成功");
    }

    public void delete(String path) throws IOException {
        Path src = new Path(path);
        fs.delete(src,true);
        System.out.println("删除成功");
    }

    public FSDataOutputStream createFile(Path path) throws IOException {	//将传入的dstPath转换为path对象
        if (fs.exists(path)) {
            boolean deleteSuccess = fs.delete(path, true); // 第二个参数为true表示递归删除目录
            if (!deleteSuccess) {
                System.err.println("Failed to delete existing file/directory.");
                return null;
            }
            System.out.println("Existing file/directory deleted successfully.");
        }
        System.out.println("创建文件成功");
        return fs.create(path);
    }

    public FileSystem getFS() {
        return fs;
    }
}
