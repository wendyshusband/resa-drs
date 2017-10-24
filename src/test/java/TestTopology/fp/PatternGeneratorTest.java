package TestTopology.fp;

import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.junit.Test;
import resa.shedding.tools.DRSzkHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by 44931 on 2017/9/9.
 */
public class PatternGeneratorTest {
    private Map<String, Integer> dict = new HashMap<>();

    public void dictTest() {
        int id = 0;
        String str = "/diu.yaml";
        System.out.println(this.getClass());
        InputStream in = this.getClass().getResourceAsStream(str);
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(this.getClass().getResourceAsStream(str)))) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                dict.put(line, id++);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (Map.Entry entry : dict.entrySet()) {
            System.out.println(entry.getKey()+"::"+entry.getValue());
        }
    }

    public static void main(String[] args) {
        PatternGeneratorTest patternGeneratorTest = new PatternGeneratorTest();
        patternGeneratorTest.dictTest();
    }
}