package common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * HashUtil.java
 *
 * วิธีใช้ตรวจความถูกต้อง
 *   String expected = HashUtil.sha256Hex(Path.of("files/test.bin"));      // ไฟล์ต้นฉบับบน server
 *   String actual   = HashUtil.sha256Hex(Path.of("downloads/test.bin"));  // ไฟล์ที่ client ได้
 *   boolean ok      = HashUtil.matches(expected, actual);
 *
 * อ่านไฟล์ทีละ chunk (BUFFER_SIZE) จึงใช้ memory คงที่ ไม่ว่าไฟล์จะใหญ่แค่ไหน
 */
public final class HashUtil
{

    private static final String ALGORITHM = "SHA-256";

    private HashUtil() {}
    /**
     * คำนวณ SHA-256 ของไฟล์ทั้งไฟล์ คืนเป็นข้อความ hex ตัวพิมพ์เล็ก 64 ตัวอักษร
     * เช่น "e3b0c44298fc1c149afbf4c8996fb924..."
     */
    public static String sha256Hex(Path file) throws IOException
    {
        try (InputStream in = Files.newInputStream(file))
        {
            return sha256Hex(in);
        }
    }

    /**
     * คำนวณ SHA-256 จาก InputStream ใด ๆ (อ่านจนจบ stream)
     */
    public static String sha256Hex(InputStream in) throws IOException
    {
        MessageDigest digest = newDigest();
        byte[] buffer = new byte[Protocol.BUFFER_SIZE];
        int n;
        while ((n = in.read(buffer)) != -1) 
        {
            digest.update(buffer, 0, n);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    /** คำนวณ SHA-256 ของ byte array (เหมาะกับข้อมูลเล็ก ๆ เช่นใช้ทดสอบ) */
    public static String sha256Hex(byte[] data) 
    {
        return HexFormat.of().formatHex(newDigest().digest(data));
    }

    /** เทียบ hash สองค่า ไม่สนตัวพิมพ์เล็ก/ใหญ่ และไม่สนช่องว่างหัวท้าย */
    public static boolean matches(String expected, String actual) 
    {
        if (expected == null || actual == null) 
        {
            return false;
        }
        return expected.trim().equalsIgnoreCase(actual.trim());
    }

    /** สร้าง MessageDigest ใหม่ทุกครั้ง เพราะ object นี้ไม่ thread-safe ห้ามแชร์ข้าม thread */
    private static MessageDigest newDigest() 
    {
        try 
        {
            return MessageDigest.getInstance(ALGORITHM);
        } 
        catch (NoSuchAlgorithmException e) 
        {
            throw new IllegalStateException(ALGORITHM + " not available", e);
        }
    }
}
