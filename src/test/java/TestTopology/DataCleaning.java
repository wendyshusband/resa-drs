package TestTopology;

import TestTopology.fp.WordList;
import TestTopology.testforls.TestRedis;
import TestTopology.testforls.TestWRInputFileForRedis;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.util.*;

/**
 * Created by 44931 on 2017/9/9.
 */
public class DataCleaning {
    private static Set<String> words = new HashSet<>();
    private static List<Integer> targetTasks = new LinkedList<>();
    private static Map<String, Integer> dict = new HashMap<>();

    static void getWord() throws IOException {
        FileWriter fileWriter = new FileWriter("E:/outlierdetection/topk/words1000.txt");
        BufferedWriter writer = new BufferedWriter(fileWriter);
        FileReader fileReader = new FileReader("E:/outlierdetection/fp/newtweet1000.txt");
        BufferedReader reader = new BufferedReader(fileReader);
        String s;
        try {
            while ((s = reader.readLine()) != null) {
                String[] arr = s.split(" ");
                for (String a : arr) {
                    writer.write(a+"\n");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            writer.flush();
            writer.close();
            System.out.println("get word done!");
        }

    }

    static void clean(String readFile, String writeFile) throws IOException {
        FileWriter fileWriter = new FileWriter(writeFile);
        BufferedWriter writer = new BufferedWriter(fileWriter);
        FileReader fileReader = new FileReader(readFile);
        BufferedReader reader = new BufferedReader(fileReader);
        String s;
        try {
            while ((s = reader.readLine()) != null) {
                s = s.replaceAll("[^a-zA-Z\\s]", " ");
                s = s.replaceAll("\\s+", " ");
                if (s.equals(" ") || s.isEmpty()) {
                    continue;
                }
                s = s.trim();
                writer.write(s + "\n");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            writer.flush();
            writer.close();
            System.out.println("done!");
        }
    }

    static void macthData(String readFile, String writeFile, String averageFile) throws IOException {
        FileWriter fileWriter = new FileWriter(writeFile);
        BufferedWriter writer = new BufferedWriter(fileWriter);

        FileWriter averageFileWriter = new FileWriter(averageFile);
        BufferedWriter averageWriter = new BufferedWriter(averageFileWriter);

        FileReader fileReader = new FileReader(readFile);
        BufferedReader reader = new BufferedReader(fileReader);

        String s;
        while ((s = reader.readLine()) != null) {
            int average = 0;
            StringBuffer res = new StringBuffer("{");
            String[] arr = s.split(" ");
            for (int i=0; i<arr.length; i++) {
                if (dict.containsKey(arr[i])) {
                    average++;
                    res.append(1);
                    res.append(",");
                } else {
                    res.append(0);
                    res.append(",");
                }
            }
            res.delete(res.length()-1, res.length());
            res.append("}");
            writer.write(res + "\n");
            double averageRes = (average * 1.0) / arr.length;
            averageWriter.write(averageRes+"\n");
        }
        writer.flush();
        writer.close();
        System.out.println("calc done!");
    }

    static void buildDict(String dictFile) throws IOException {
        int id = 0;
        FileReader fileReader = new FileReader(dictFile);
        BufferedReader reader = new BufferedReader(fileReader);
        String s;
        while ((s = reader.readLine()) != null) {
            s = s.trim();
            dict.put(s,id++);
        }
        System.out.println("build dict done!");
    }

    public static void main(String[] args) throws IOException {

//        int number = 200000;
//        String dictFile = "E:/outlierdetection/fp/dict-10000.txt";
//        String readFile = "E:/outlierdetection/fp/tweet.txt";
//        String writeFile = "E:/outlierdetection/fp/newtweet.txt";
//        String vectorFile = "E:/outlierdetection/fp/fpvector10000.txt";
//        String averageFile = "E:/outlierdetection/fp/fpaverage10000.txt";
//        String newFile = "E:/outlierdetection/fp/newtweet"+number+".txt";
//        //buildDict(dictFile);
//        //clean(readFile, writeFile);
//        //macthData(writeFile, vectorFile, averageFile);
//        //copyToNewFile(writeFile,newFile,number);
//        dictFile = dictFile.replaceAll("\\p{P}|\\p{S}", " ");
//        System.out.println(dictFile);
        //getWord();
        Jedis jedis = TestRedis.getJedis();
        int count = 0;
        while (true) {
            count++;
            String a = jedis.lpop("topk");
            //System.out.println(a);
            if (a != null) {
                byte[] message = (a+"\n").getBytes();//shedstorm1 originstorm1
                TestWRInputFileForRedis
                        .appendFile("E:/outlierdetection/topk/benchmark/topk11479.txt",message,1);
//                TestWRInputFileForRedis
//                        .appendFile("/home/tkl/cleantweet",message,1);
            } else {
                break;
            }
        }
        System.out.println(count);
    }

    private static void copyToNewFile(String readFile, String writeFile, int number) throws IOException {
        FileWriter fileWriter = new FileWriter(writeFile);
        BufferedWriter writer = new BufferedWriter(fileWriter);
        FileReader fileReader = new FileReader(readFile);
        BufferedReader reader = new BufferedReader(fileReader);
        String s ;
        int i = 0;
        while ((s = reader.readLine()) != null && i < number) {
            System.out.println(s);
            writer.append(s+"\n");
            i++;
        }
        writer.flush();
        writer.close();
        System.out.println(i);
    }

    private static void emitSubPattern(int[] wordIds) {
        int n = wordIds.length;
        int[] buffer = new int[n];
        ArrayList<WordList>[] wordListForTargetTask = new ArrayList[targetTasks.size()];

        for (int i = 1; i < (1 << n); i++) {
            int k = 0;
            for (int j = 0; j < n; j++) {
                if ((i & (1 << j)) > 0) {
                    buffer[k++] = wordIds[j];
                }
            }
            WordList wl = new WordList(Arrays.copyOf(buffer, k));
            int targetIndex = WordList.getPartition(targetTasks.size(), wl);
            if (wordListForTargetTask[targetIndex] == null) {
                wordListForTargetTask[targetIndex] = new ArrayList<>();
            }
            wordListForTargetTask[targetIndex].add(wl);
        }
        for (int i = 0; i < wordListForTargetTask.length; i++) {
            if (wordListForTargetTask[i] != null && wordListForTargetTask[i].size() > 0) {
                System.out.println("target task:"+targetTasks.get(i));
                List arrayList = Arrays.asList(wordListForTargetTask[i]);
                for (Object input : arrayList) {
                    System.out.print(input+"~");
                }
                System.out.println();
            }
        }
    }

    private static Integer word2Id(String word) {
        return dict.get(word);
    }
}
