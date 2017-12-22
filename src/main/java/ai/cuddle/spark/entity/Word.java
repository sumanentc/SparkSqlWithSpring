package ai.cuddle.spark.entity;

/**
 * Created by suman.das on 11/30/17.
 */
public class Word {
    private String word;

    public Word() {
    }

    public Word(String word) {
        this.word = word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public String getWord() {
        return word;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Word{");
        sb.append("word='").append(word).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
