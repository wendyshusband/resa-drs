package TestTopology.testforls;

import org.apache.storm.utils.Utils;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import resa.shedding.tools.TestRedis;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by kailin on 2017/7/22.
 */
public class TestWRInputFileForRedis {

    public static boolean appendFile(String filename, byte[] message, int size) {
        try {
            RandomAccessFile randomFile = new RandomAccessFile(filename, "rw");
            long fileLength = randomFile.length();
            randomFile.seek(fileLength);
            FileChannel fc = randomFile.getChannel();
            ByteBuffer bb = ByteBuffer.allocate(message.length);
            for (int i=0; i<size; i++) {
                bb.put(message);
                bb.flip();
                fc.write(bb);
                bb.rewind();
            }
            randomFile.close();
            return true;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static boolean writeFile(String filename, byte[] message, int size) {
        try {
            RandomAccessFile randomFile = new RandomAccessFile(filename, "rw");
            long fileLength = randomFile.length();
            randomFile.seek(0);
            //randomFile.seek(fileLength);
            FileChannel fc = randomFile.getChannel();
            ByteBuffer bb = ByteBuffer.allocate(message.length);
            for (int i=0; i<size; i++) {
                bb.put(message);
                bb.flip();
                fc.write(bb);
                bb.rewind();
            }
            return true;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

//    private static ByteBuffer readFile(String filename, long mark) {
//        RandomAccessFile randomFile = null;
//        try {
//            randomFile = new RandomAccessFile(filename, "rw");
//            long fileLength = randomFile.length();
//            randomFile.seek(mark);
//            FileChannel fc = randomFile.getChannel();
//            System.out.println(fc.size());
//            System.out.println(fileLength);
//            System.out.println(mark);
//            System.out.println((int) (fileLength - mark));
//            //ByteBuffer bb = ByteBuffer.allocate((int) (fileLength - mark));
//            ByteBuffer bb = ByteBuffer.allocate(10);
//            fc.read(bb);
////                int readFlag = fc.read(bb);
////                if (readFlag == -1) {
////                    break;
////                }
//            bb.flip();
////                while (bb.hasRemaining()) {
////
////                    System.out.println(bb.get() + "    " + bb.position() +  "   " + bb.remaining());
////                }
//            bb.clear();
//            randomFile.close();
//            return bb;
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
    static List readFileByLine (String filename, int size) {
        int bufSize = size;
        File fin = new File(filename);
        Date startDate = new Date();
        List<String> data = null;
        try {
            FileChannel fcin = new RandomAccessFile(fin, "r").getChannel();
            ByteBuffer readBuffer = ByteBuffer.allocate(bufSize);
            data = readFile(fcin,readBuffer);
            Date endDate = new Date();
            if(fcin.isOpen()){
                fcin.close();
            }
            //System.out.println(endDate+"~~~"+startDate);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    private static List readFile(FileChannel fcin, ByteBuffer readBuffer) {
        String enter = "\r\n";
        List<String> dataList = new ArrayList<String>();//存储读取的每行数据
        byte[] lineByte = new byte[0];

        String encode = "GBK";
//      String encode = "UTF-8";
        try {
            //temp：由于是按固定字节读取，在一次读取中，第一行和最后一行经常是不完整的行，因此定义此变量来存储上次的最后一行和这次的第一行的内容，
            //并将之连接成完成的一行，否则会出现汉字被拆分成2个字节，并被提前转换成字符串而乱码的问题
            byte[] temp = new byte[0];
            while (fcin.read(readBuffer) != -1) {//fcin.read(rBuffer)：从文件管道读取内容到缓冲区(rBuffer)
                int rSize = readBuffer.position();//读取结束后的位置，相当于读取的长度
                byte[] bs = new byte[rSize];//用来存放读取的内容的数组
                readBuffer.rewind();//将position设回0,所以你可以重读Buffer中的所有数据,此处如果不设置,无法使用下面的get方法
                readBuffer.get(bs);//相当于rBuffer.get(bs,0,bs.length())：从position初始位置开始相对读,读bs.length个byte,并写入bs[0]到bs[bs.length-1]的区域
                readBuffer.clear();

                int startNum = 0;
                int LF = 10;//换行符
                int CR = 13;//回车符
                boolean hasLF = false;//是否有换行符
                for(int i = 0; i < rSize; i++){
                    if(bs[i] == LF){
                        hasLF = true;
                        int tempNum = temp.length;
                        int lineNum = i - startNum;
                        lineByte = new byte[tempNum + lineNum];//数组大小已经去掉换行符

                        System.arraycopy(temp, 0, lineByte, 0, tempNum);//填充了lineByte[0]~lineByte[tempNum-1]
                        temp = new byte[0];
                        System.arraycopy(bs, startNum, lineByte, tempNum, lineNum);//填充lineByte[tempNum]~lineByte[tempNum+lineNum-1]

                        String line = new String(lineByte, 0, lineByte.length, encode);//一行完整的字符串(过滤了换行和回车)
                        dataList.add(line);
                        //System.out.println("aaa "+line);

                        //过滤回车符和换行符
                        if(i + 1 < rSize && bs[i + 1] == CR){
                            startNum = i + 2;
                        }else{
                            startNum = i + 1;
                        }

                    }
                }
                if(hasLF){
                    temp = new byte[bs.length - startNum];
                    System.arraycopy(bs, startNum, temp, 0, temp.length);
                }else{//兼容单次读取的内容不足一行的情况
                    byte[] toTemp = new byte[temp.length + bs.length];
                    System.arraycopy(temp, 0, toTemp, 0, temp.length);
                    System.arraycopy(bs, 0, toTemp, temp.length, bs.length);
                    temp = toTemp;
                }
            }
            if(temp != null && temp.length > 0){//兼容文件最后一行没有换行的情况
                String line = new String(temp, 0, temp.length, encode);
                dataList.add(line);
                //System.out.println("sss "+line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataList;
    }

//    @Test
//    public void read() {
//        List<String> data = TestWRInputFileForRedis.readFileByLine("E:/outlierdetection/111.txt",1);
//        //String s = new String(byteBuffer.array());
//        for (String s : data) {
//            System.out.println(s);
//        }
//        //System.out.println("sssab"+data+"sssa");
//    }

    public static void main(String[] args) {
        int size = 1000000;
        //byte[] message = {'f','u','c','k','\n','c','o','m','e','\n'};
        //byte[] message = {'f','u','c','k',' ','c','o','m','e','\r','\n'};
        Jedis jedis= TestRedis.getJedis();
        long start = System.currentTimeMillis();
        int count = 0;
//        while (true) {
//            count++;
//            String a = jedis.lpop("fpres");
//            //System.out.println(a);
//            if (a != null) {
//                byte[] message = (a+"\r\n").getBytes();//shedstorm1 originstorm1
//                TestWRInputFileForRedis
//                        .appendFile("E:/outlierdetection/fp/result/benchmark100002.txt",message,1);
//            } else {
//                break;
//            }
//        }
//        System.out.println("finish fpres "+count);

//        count = 0;
//        while (true) {
//            count++;
//            String a = jedis.lpop("build");
//            //System.out.println(a);
//            if (a != null) {
//                byte[] message = (a+"\r\n").getBytes();//shedstorm1 originstorm1
//                TestWRInputFileForRedis
//                        .appendFile("E:/outlierdetection/fp/result/ershiwan.txt",message,1);
//            } else {
//                break;
//            }
//        }
//        System.out.println("finish build "+count);

        while (true) {
            count++;
            String a = jedis.rpop(args[1]);
            //System.out.println(a);
            if (a != null) {
                byte[] message = (a+"\r\n").getBytes();//shedstorm1 originstorm1
                TestWRInputFileForRedis
                        .appendFile(args[0],message,1);
            } else {
                break;
            }
        }
        System.out.println("finish full "+count);

//        System.out.println("time:"+(System.currentTimeMillis()-start));
        //System.out.println(TestWRInputFileForRedis.writeFile("E:/testData/testRedis.txt", message, size));
        //System.out.println(TestWRInputFileForRedis.appendFile("E:/testData/testRedis.txt", message, size));
        //TestWRInputFileForRedis.readFile("E:/testData/testRedis.txt", 0);

    }
}
