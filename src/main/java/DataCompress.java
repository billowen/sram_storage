import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.Scanner;

public class DataCompress {
    public static void main(String[] args) throws IOException {
        File[] files = new File("data/Yield_5%").listFiles();
        long count1 = 0;
        long count2 = 0;
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
                    count1 = compress(bytes).length * 16 * 16;
                } else {
                    count2 += compress(bytes).length * 16 * 16;
                }
            }
        }
        System.out.println(count1 / (double) 1024 / (double) 1024);
        System.out.println(count2 / (double) 1024 / (double) 1024);
    }


    public static byte[] compress(byte srcBytes[]) throws IOException {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        LZ4Compressor compressor = factory.highCompressor();
        LZ4BlockOutputStream compressedOutput = new LZ4BlockOutputStream(
                byteOutput, 2048, compressor);
        compressedOutput.write(srcBytes);
        compressedOutput.close();
        return byteOutput.toByteArray();
    }

}
