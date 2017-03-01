import com.phynum.elasticsearch.BatchReader;

/**
 * Created by thomasdutta on 3/1/17.
 */
public class Application {
    public static void main(String[] args) {
        Thread thread1 = new Thread(new BatchReader());
        thread1.start();
    }

}
