import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class ClientTest {
    @Test
    public void testParser() {
        String message="+FULLRESYNC aigd3mysfcggmir27nsdq51q2h4aagd2x81j1mb6";
        String regex="^\\+FULLRESYNC ([a-zA-Z0-9]+)$";
        Pattern pattern = Pattern.compile(regex);
        System.out.println(pattern.matcher(message).matches());
    }
}
