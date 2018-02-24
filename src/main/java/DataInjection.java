import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.Scanner;

public class DataInjection {
    public static void main(String[] args) throws IOException {
        try (Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build()) {
            Session session = cluster.connect("sram_test");
            File[] files = new File("data/Yield_50%").listFiles();
            for (File file : files) {
                Path p = file.toPath();
                String failType = p.getFileName().toString();
                try (BufferedReader reader = Files.newBufferedReader(p)) {
                    BitSet bits = new BitSet();
                    String l;
                    while ((l = reader.readLine()) != null) {
                        Scanner s = new Scanner(l.trim()).useDelimiter(",");
                        int x = s.nextInt();
                        int y = s.nextInt();
                        bits.set(y * 1024 + x);
                    }
                    byte[] bytes = bits.toByteArray();
                    if (failType.equals("All")) {
                        for (int blockX = 0; blockX < 16; blockX++) {
                            for (int blockY = 0; blockY < 16; blockY++) {
                                session.execute("INSERT INTO origin (wafer_id, die_x, die_y, block_x, block_y, data) " +
                                        "VALUES (?, ?, ?, ?, ?, ?)", 1, 1, 1, blockX, blockY, ByteBuffer.wrap(bytes));
                            }
                        }
                    } else {
                        for (int blockX = 0; blockX < 16; blockX++) {
                            for (int blockY = 0; blockY < 16; blockY++) {
                                session.execute("INSERT INTO result (wafer_id, die_x, die_y, block_x, block_y, fail_type, data) " +
                                        "VALUES (?, ?, ?, ?, ?, ?, ?)", 1, 1, 1, blockX, blockY, failType, ByteBuffer.wrap(bytes));
                            }
                        }
                    }
                }
            }
        }
    }
}
