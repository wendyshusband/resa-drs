package TestTopology.fp;

import org.apache.storm.serialization.SerializableSerializer;
import com.esotericsoftware.kryo.DefaultSerializer;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Created by ding on 14-6-5.
 */
@DefaultSerializer(SerializableSerializer.class)
public class WordList implements Serializable {

    private int[] words;

    public WordList(int... words) {
        this.words = words;
    }

    public int[] getWords() {
        return words;
    }

    @Override
    public int hashCode() {
        return words == null ? 0 : Arrays.hashCode(words);
    }

    @Override
    public boolean equals(Object obj) {
        return Arrays.equals(words, ((WordList) obj).words);
    }

    @Override
    public String toString() {
        return Arrays.toString(words);
    }

    public static int getPartition(int numPart, WordList wordList) {
        return Math.abs(wordList.hashCode()) % numPart;
    }

    public int compare(WordList pattern) {
        if (this.words.length == pattern.getWords().length && Arrays.equals(this.words,pattern.getWords())) {
            return 0; // pattern == this
        } else if (this.words.length > pattern.getWords().length
                && Arrays.asList(this.words).containsAll(Arrays.asList(pattern.getWords()))) {
            return 1; // this.words is a super pattern
        } else if (this.words.length < pattern.getWords().length
                && Arrays.asList(pattern.getWords()).containsAll(Arrays.asList(this.getWords()))) {
            return -1; // this.words is a sub pattern
        }
        return Integer.MAX_VALUE;
    }
}
