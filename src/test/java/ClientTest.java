import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class ClientTest {
    @Test
    public void testParser() {
        String[] result = Main.splitCommand("*1\r\nPING\r\n");
        assertArrayEquals(new String[]{"*1", "PING"}, result);
        result=Main.splitCommand("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n");
        assertArrayEquals(new String[]{"*2", "$4", "ECHO", "$3", "hey"}, result);
    }
}
