package com.vita.bigdata.collect;

import java.io.*;

/**
 * 编写数据生成模拟程序
 */
public class ReadWriteLog {

    public static String readFileName;

    public static String writeFileName;

    public static void main(String[] args) {
        readFileName = args[0];
        writeFileName = args[1];

        try {
            readFileByLines(readFileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void readFileByLines(String fileName) {
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        String tempString = null;

        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            fis = new FileInputStream(fileName);
            //// 从文件系统中的某个文件中获取字节
            isr = new InputStreamReader(fis, "GBK");
            br = new BufferedReader(isr);
            int count = 0;
            while ((tempString = br.readLine()) != null) {
                count++;
                //显示行号
                Thread.sleep(300);
                String str = new String(tempString.getBytes("GBK"), "UTF8");
                System.out.println("row:"+count+">>>>>>>>"+str);
                writeFile(writeFileName, str);
            }
            isr.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (isr != null) {
                try {
                    isr.close();
                } catch (IOException e1) {
                }
            }
        }
    }

    public static void writeFile(String file, String content) {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
            out.write("\n");
            out.write(content);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
