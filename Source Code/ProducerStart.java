import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ProducerStart {
    public static void main(String[] args) throws IOException{
        String topic = "Twitter";
        List<String> keyword= Arrays.asList("covid","covid19","corona");
        Twitter_API  Producer = new Twitter_API(topic, keyword);
        Producer.run();
    }
}
