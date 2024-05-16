package org.urbcomp.startdb.selfstar.utils;

import java.io.*;

public class ByteArrayFileReader {
    private DataInputStream dis;
    private boolean isEOF = false;

    public ByteArrayFileReader(String filePath) throws FileNotFoundException {
        FileInputStream fis = new FileInputStream(filePath);
        BufferedInputStream bis = new BufferedInputStream(fis);
        this.dis = new DataInputStream(bis);
    }

    public byte[] readNextBytes() throws IOException {
        if (isEOF || dis == null) {
            return null;  // 如果已到达文件末尾或流已关闭，返回null
        }

        try {
            int length = dis.readInt();  // 读取长度
            byte[] data = new byte[length];
            dis.readFully(data);  // 根据长度读取数据
            return data;
        } catch (EOFException e) {
            isEOF = true;  // 设置文件末尾标志
            close();  // 到达文件末尾时关闭流
            return null;  // 返回null表示没有更多数据可读
        }
    }

    public void close() throws IOException {
        if (dis != null) {
            dis.close();
            dis = null;
        }
    }


}
