package ai.cuddle.spark.entity;

import java.io.Serializable;

/**
 * Created by suman.das on 11/30/17.
 */
public class Count implements Serializable {
    private String word;
    private long count;

    public Count() {
    }

    public Count(String word, long count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Count{");
        sb.append("word='").append(word).append('\'');
        sb.append(", count=").append(count);
        sb.append('}');
        return sb.toString();
    }
}
