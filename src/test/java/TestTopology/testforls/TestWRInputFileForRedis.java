package TestTopology.testforls;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by kailin on 2017/7/22.
 */
public class TestWRInputFileForRedis {

    private static boolean appendFile(String filename, byte[] message, int size) {
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
            return true;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private static boolean writeFile(String filename, byte[] message, int size) {
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
//            System.out.println(fileLength);
//            System.out.println(mark);
//            System.out.println((int) (fileLength - mark));
//            ByteBuffer bb = ByteBuffer.allocate((int) (fileLength - mark));
//            while (true) {
//                int readFlag = fc.read(bb);
//                if (readFlag == -1) {
//                    break;
//                }
//                bb.flip();
//                while (bb.hasRemaining()) {
//
//                    System.out.println(bb.get() + "    " + bb.position() +  "   " + bb.remaining());
//                }
//                bb.clear();
//            }
//            randomFile.close();
//            return bb;
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }

    public static void main(String[] args) {
        int size = 1000000;
        //byte[] message = {'f','u','c','k','\n','c','o','m','e','\n'};
        byte[] message = {'f','u','c','k',' ','c','o','m','e','\r','\n'};
        System.out.println(TestWRInputFileForRedis.writeFile("E:/testData/testRedis.txt", message, size));
        //System.out.println(TestWRInputFileForRedis.appendFile("E:/testData/testRedis.txt", message, size));
        //TestWRInputFileForRedis.readFile("E:/testData/testRedis.txt", 0);

    }
}
