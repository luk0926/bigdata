package day01;

/**
 * @BelongsProject: bigdata
 * @BelongsPackage: day01
 * @Author: luk
 * @CreateTime: 2020/3/17 16:13
 */
public class WordCountJava {
    private String word;
    private int count;

    public WordCountJava() {
    }

    public WordCountJava(String word, int count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordCountJava{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
