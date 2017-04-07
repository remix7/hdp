package cn.echoes.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

/**
 * -------------------------------------
 * TODO
 * -------------------------------------
 * Created by liutao on 2017/2/27 15:33.
 */
public class FileUtils {
    /**
     * 上传文件到hdfs
     */
    public static void uploadFile(Configuration conf, String uri, String local, String remote) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        fs.close();
    }

    /**
     * 湖区hdfs上传刘流
     */
    public static void getFileStream(Configuration conf, String uri, String local, String remote) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(remote);
        FSDataInputStream in = fs.open(path); //获取文件流
        FileOutputStream fos = new FileOutputStream("");//输出流

        int c = 0;
        while ((c = in.read()) != -1) {
            fos.write(c);
        }
        in.close();
        fos.close();

    }

    /**
     * 创建文件夹
     */
    public static void markDir(Configuration conf, String uri, String remoteFile) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(remoteFile);
        fs.mkdirs(path);

    }

    /**
     * 查看文件
     */
    public static void cat(Configuration conf, String uri, String remoteFile) throws IOException {
        Path path = new Path(remoteFile);
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FSDataInputStream fsdir = null;
        try {
            fsdir = fs.open(path);
            IOUtils.copyBytes(fsdir, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(fsdir);
            fs.close();
        }
    }

    /**
     * 下载文件
     */
    public static void download(Configuration conf, String uri, String remote, String local) throws IOException {
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        fs.copyToLocalFile(path, new Path(local));
        fs.close();
    }

    /**
     * 删除文件或文件夹
     */
    public static void delete(Configuration conf, String uri, String filePath) throws IOException {
        Path path = new Path(filePath);
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        fs.deleteOnExit(path);
        fs.close();
    }

    /**
     * 查看目录下的文件
     */
    public static void ls(Configuration conf, String uri, String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FileStatus[] list = fs.listStatus(path);
        for (FileStatus f : list) {

        }
        fs.close();

    }

}
