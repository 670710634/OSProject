package common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Protocol.java
 * ------------------------------------------------------------
 * ไฟล์กลางที่ทั้ง Client และ Server ใช้ร่วมกัน เก็บ
 *   1) ค่าคงที่ (ชื่อคำสั่ง, รหัส error, ขนาด buffer)
 *   2) ตัวแปลง request/response ระหว่าง "ข้อความ" กับ "ข้อมูลใน Java"
 *   3) readLine() สำหรับอ่านบรรทัดคำสั่งจาก socket
 *
 * รูปแบบ Protocol (ข้อความล้วน ลงท้ายบรรทัดด้วย '\n' เข้ารหัส UTF-8)
 *
 *   Client -> Server              Server -> Client
 *   ---------------------------   ------------------------------------------
 *   LIST                          FILE <name> <size>   (ซ้ำทีละไฟล์)
 *                                 END                  (จบรายการ)
 *
 *   INFO <filename>               SIZE <bytes>
 *                                 หรือ ERROR <code> <message>
 *
 *   GET <filename> <offset> <len> OK <len>\n  ตามด้วย payload ดิบ <len> byte
 *                                 หรือ ERROR <code> <message>  (ไม่มี payload)
 *
 * หมายเหตุ: ชื่อไฟล์ห้ามมีช่องว่าง (เพื่อให้ parse ง่าย)
 */
public final class Protocol 
{

    private Protocol() {} // ไม่ให้สร้าง object ใช้เป็นคลาสเครื่องมืออย่างเดียว

    // ---------- ค่าคงที่ทั่วไป ----------
    public static final int DEFAULT_PORT = 9000;
    public static final int BUFFER_SIZE = 64 * 1024;   // ขนาด buffer ตอนอ่าน/เขียน 64 KB
    public static final int MAX_LINE_LENGTH = 4096;    // กันคำสั่งยาวผิดปกติ

    // ---------- ชื่อคำสั่งของ Client ----------
    public static final String CMD_LIST = "LIST";
    public static final String CMD_INFO = "INFO";
    public static final String CMD_GET = "GET";

    // ---------- คำตอบของ Server ----------
    public static final String RES_FILE = "FILE";
    public static final String RES_END = "END";
    public static final String RES_SIZE = "SIZE";
    public static final String RES_OK = "OK";
    public static final String RES_ERROR = "ERROR";

    // ---------- รหัส error ----------
    public static final int ERR_BAD_REQUEST = 400; // คำสั่งผิดรูปแบบ / ตัวเลขไม่ถูกต้อง
    public static final int ERR_NOT_FOUND = 404;   // ไม่พบไฟล์
    public static final int ERR_BAD_RANGE = 416;   // offset/length เกินขนาดไฟล์
    public static final int ERR_INTERNAL = 500;    // server ผิดพลาดเอง

    // ============================================================
    // ชนิดข้อมูล
    // ============================================================

    /** Exception สำหรับ "ข้อผิดพลาดของ protocol" พกรหัส error ไว้ให้ผู้จัดการนำไปใช้ต่อ */
    public static class ProtocolException extends Exception 
    {
        private static final long serialVersionUID = 1L;
        private final int code;

        public ProtocolException(int code, String message) 
        {
            super(message);
            this.code = code;
        }

        public int getCode() 
        {
            return code;
        }
    }

    /** request ที่ Server parse ได้ (ช่องที่ไม่ใช้จะเป็น null / 0) */
    public record Request(String command, String filename, long offset, long length) {}

    /** ข้อมูลหนึ่งบรรทัดของ LIST */
    public record FileInfo(String name, long size) {}

    // ============================================================
    // ฝั่ง Client: สร้างข้อความ request
    // ============================================================

    public static String listRequest() 
    {
        return CMD_LIST + "\n";
    }

    public static String infoRequest(String filename) 
    {
        return CMD_INFO + " " + filename + "\n";
    }

    public static String getRequest(String filename, long offset, long length) 
    {
        return CMD_GET + " " + filename + " " + offset + " " + length + "\n";
    }

    // ============================================================
    // ฝั่ง Server: แปลงบรรทัดคำสั่งเป็น Request
    // ============================================================

    /**
     * แปลงบรรทัดที่ client ส่งมาเป็น Request
     * ถ้ารูปแบบผิดจะโยน ProtocolException(400) เพื่อให้ server ตอบ ERROR กลับไป
     */
    public static Request parseRequest(String line) throws ProtocolException 
    {
        if (line == null || line.isBlank()) 
        {
            throw new ProtocolException(ERR_BAD_REQUEST, "empty request");
        }
        String[] parts = line.trim().split(" ");
        String cmd = parts[0].toUpperCase();

        switch (cmd) 
        {
            case CMD_LIST:
                if (parts.length != 1) 
                {
                    throw new ProtocolException(ERR_BAD_REQUEST, "usage: LIST");
                }
                return new Request(CMD_LIST, null, 0, 0);

            case CMD_INFO:
                if (parts.length != 2) 
                {
                    throw new ProtocolException(ERR_BAD_REQUEST, "usage: INFO <filename>");
                }
                return new Request(CMD_INFO, parts[1], 0, 0);

            case CMD_GET:
                if (parts.length != 4) 
                {
                    throw new ProtocolException(ERR_BAD_REQUEST, "usage: GET <filename> <offset> <length>");
                }
                long offset = parseNumber(parts[2], "offset");
                long length = parseNumber(parts[3], "length");
                return new Request(CMD_GET, parts[1], offset, length);

            default:
                throw new ProtocolException(ERR_BAD_REQUEST, "unknown command: " + parts[0]);
        }
    }

    /** แปลงสตริงเป็นเลขที่ไม่ติดลบ ถ้าไม่ใช่เลขจะโยน 400 */
    private static long parseNumber(String text, String fieldName) throws ProtocolException 
    {
        try 
        {
            long value = Long.parseLong(text);
            if (value < 0) 
            {
                throw new ProtocolException(ERR_BAD_REQUEST, fieldName + " must not be negative");
            }
            return value;
        } 
        catch (NumberFormatException e) 
        {
            throw new ProtocolException(ERR_BAD_REQUEST, fieldName + " is not a number: " + text);
        }
    }

    // ============================================================
    // ฝั่ง Server: สร้างข้อความ response
    // ============================================================

    public static String fileLine(String name, long size) 
    {
        return RES_FILE + " " + name + " " + size + "\n";
    }

    public static String endLine() 
    {
        return RES_END + "\n";
    }

    public static String sizeResponse(long size) 
    {
        return RES_SIZE + " " + size + "\n";
    }

    /** header ของ GET ที่สำเร็จ: ต่อด้วย payload จำนวน length byte */
    public static String okResponse(long length) 
    {
        return RES_OK + " " + length + "\n";
    }

    public static String errorResponse(int code, String message) 
    {
        return RES_ERROR + " " + code + " " + message + "\n";
    }

    // ============================================================
    // ฝั่ง Client: แปลงข้อความ response กลับเป็นข้อมูล
    // ============================================================

    /** ถ้าบรรทัดเป็น "ERROR <code> <msg>" จะโยน ProtocolException พร้อมรหัสนั้น */
    private static void throwIfError(String line) throws ProtocolException 
    {
        if (line.startsWith(RES_ERROR + " ")) 
        {
            String[] p = line.split(" ", 3);
            int code = ERR_INTERNAL;
            try
            {
                code = Integer.parseInt(p[1]);
            } 
            catch (NumberFormatException ignored) 
            {
                // ถ้ารหัสอ่านไม่ได้ ใช้ 500 ไปก่อน
            }
            String msg = p.length > 2 ? p[2] : "";
            throw new ProtocolException(code, msg);
        }
    }

    /** แปลงคำตอบของ INFO ("SIZE <bytes>") เป็นขนาดไฟล์ */
    public static long parseSizeResponse(String line) throws ProtocolException 
    {
        if (line == null) 
        {
            throw new ProtocolException(ERR_INTERNAL, "connection closed by server");
        }
        throwIfError(line);
        String[] p = line.split(" ");
        if (p.length != 2 || !p[0].equals(RES_SIZE)) 
        {
            throw new ProtocolException(ERR_INTERNAL, "unexpected response: " + line);
        }
        return parseNumber(p[1], "size");
    }

    /** แปลง header ของ GET ("OK <length>") เป็นจำนวน byte ของ payload ที่จะตามมา */
    public static long parseOkResponse(String line) throws ProtocolException 
    {
        if (line == null) 
        {
            throw new ProtocolException(ERR_INTERNAL, "connection closed by server");
        }
        throwIfError(line);
        String[] p = line.split(" ");
        if (p.length != 2 || !p[0].equals(RES_OK)) 
        {
            throw new ProtocolException(ERR_INTERNAL, "unexpected response: " + line);
        }
        return parseNumber(p[1], "length");
    }

    /** แปลงบรรทัด "FILE <name> <size>" ของ LIST */
    public static FileInfo parseFileLine(String line) throws ProtocolException 
    {
        String[] p = line.split(" ");
        if (p.length != 3 || !p[0].equals(RES_FILE)) 
        {
            throw new ProtocolException(ERR_INTERNAL, "unexpected response: " + line);
        }
        return new FileInfo(p[1], parseNumber(p[2], "size"));
    }

    // ============================================================
    // อ่านบรรทัดจาก InputStream
    // ============================================================

    /**
     * อ่านข้อความจนเจอ '\n' แล้วคืนเป็น String (ไม่รวม '\n' และ '\r')
     * คืน null ถ้าอีกฝ่ายปิด connection ก่อนส่งอะไรมาเลย
     *
     * ทำไมไม่ใช้ BufferedReader.readLine()?
     *   เพราะ BufferedReader จะ "อ่านล่วงหน้า" เกินบรรทัดมาเก็บใน buffer ของมัน
     *   ถ้าหลัง header "OK <len>" มี payload ดิบตามมา ข้อมูลส่วนหนึ่งจะหลุดเข้าไปอยู่ใน
     *   buffer นั้น แล้ว client จะอ่าน payload ไม่ครบ/เพี้ยน
     *   ฟังก์ชันนี้อ่านทีละ byte เท่าที่จำเป็น จึงหยุดตรง '\n' พอดี
     *   (ใช้กับ InputStream ที่ไม่ได้ห่อ buffer ไว้เท่านั้น)
     */
    public static String readLine(InputStream in) throws IOException 
    {
        ByteArrayOutputStream buf = new ByteArrayOutputStream(64);
        int b;
        while ((b = in.read()) != -1) 
        {
            if (b == '\n') 
            {
                break;
            }
            if (buf.size() >= MAX_LINE_LENGTH) 
            {
                throw new IOException("line too long");
            }
            buf.write(b);
        }
        if (b == -1 && buf.size() == 0) 
        {
            return null; // connection ถูกปิดแล้ว
        }
        String line = buf.toString(StandardCharsets.UTF_8);
        if (line.endsWith("\r")) 
        {
            line = line.substring(0, line.length() - 1);
        }
        return line;
    }
}