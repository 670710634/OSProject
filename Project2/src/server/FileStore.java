package server;

import common.Protocol;
import common.Protocol.FileInfo;
import common.Protocol.ProtocolException;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * FileStore.java
 * ------------------------------------------------------------
 * "ประตู" เดียวที่ server ใช้เข้าถึงไฟล์ใน disk หน้าที่
 *   1) list()     รายชื่อ + ขนาดไฟล์ในโฟลเดอร์ที่ตั้งไว้ (ใช้ตอบ LIST)
 *   2) resolve()  แปลงชื่อไฟล์ที่ client ส่งมา เป็น Path จริง (ใช้ตอบ INFO / GET)
 *
 * ทำไมต้องมีคลาสนี้: ชื่อไฟล์มาจาก client ซึ่งเชื่อไม่ได้
 * ถ้านำไปต่อ path ตรง ๆ client ส่ง "../../etc/passwd" แล้วอ่านไฟล์นอกโฟลเดอร์ได้ (path traversal)
 * จึงตรวจสองชั้น
 *   ชั้น 1  ตรวจ "รูปร่างของชื่อ"  ห้ามมี / \ หรือเป็น . .. (ชื่อไฟล์ล้วน ๆ เท่านั้น)
 *   ชั้น 2  ตรวจ "ปลายทางจริง"    แปลง symbolic link แล้วต้องยังอยู่ใต้โฟลเดอร์ root
 *           (กันกรณีมี symlink ในโฟลเดอร์ที่ชี้ออกไปนอก root)
 *
 * รองรับเฉพาะไฟล์ในชั้นเดียวของ root (ไม่เข้าโฟลเดอร์ย่อย) ตามขอบเขตของโปรโตคอล
 * ไม่มี state ที่เปลี่ยนค่าหลังสร้าง เรียกจากหลาย thread พร้อมกันได้อย่างปลอดภัย
 */
public final class FileStore 
{

    private final Path root; // path จริงของโฟลเดอร์ (ผ่าน toRealPath แล้ว)

    /**
     * @param root โฟลเดอร์ไฟล์ของ server ต้องมีอยู่จริงและเป็นโฟลเดอร์
     * @throws IOException ถ้าไม่พบโฟลเดอร์ หรือไม่ใช่โฟลเดอร์
     */
    public FileStore(Path root) throws IOException 
    {
        if (!Files.isDirectory(root)) 
        {
            throw new IOException("not a directory: " + root.toAbsolutePath());
        }
        this.root = root.toRealPath();
    }

    public Path getRoot() 
    {
        return root;
    }

    // ============================================================
    // LIST
    // ============================================================

    /**
     * คืนรายการไฟล์ปกติใน root เรียงตามชื่อ
     * ข้ามไฟล์ที่ชื่อมีช่องว่าง เพราะ protocol แยกคำด้วยช่องว่าง ส่งชื่อแบบนั้นไปแล้ว client parse ไม่ได้
     * ข้ามไฟล์ที่ resolve() จะปฏิเสธด้วย เพื่อให้ "สิ่งที่ LIST บอก" โหลดได้จริงทุกไฟล์
     */
    public List<FileInfo> list() throws IOException 
    {
        List<FileInfo> result = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(root)) 
        {
            for (Path p : stream) 
            {
                String name = p.getFileName().toString();
                if (hasWhitespace(name)) 
                {
                    continue;
                }
                try 
                {
                    Path real = resolve(name);
                    result.add(new FileInfo(name, Files.size(real)));
                } 
                catch (ProtocolException e) 
                {
                    // ไม่ใช่ไฟล์ปกติ หรือ symlink ชี้ออกนอก root ข้ามไป
                }
            }
        }
        result.sort(Comparator.comparing(FileInfo::name));
        return result;
    }

    // ============================================================
    // resolve: ชื่อจาก client -> Path จริง
    // ============================================================

    /**
     * แปลงชื่อไฟล์เป็น Path จริงที่ปลอดภัย
     *
     * @throws ProtocolException 400 ถ้าชื่อมีรูปร่างไม่ปลอดภัย (มี / \ หรือเป็น . ..)
     *                           404 ถ้าไม่พบไฟล์ ไม่ใช่ไฟล์ปกติ หรือหลุดออกนอก root
     */
    public Path resolve(String filename) throws ProtocolException 
    {
        // ชั้น 1: ตรวจรูปร่างของชื่อ
        if (filename == null || filename.isEmpty()
                || filename.equals(".") || filename.equals("..")
                || filename.indexOf('/') >= 0 || filename.indexOf('\\') >= 0
                || filename.indexOf('\0') >= 0) 
        {
            throw new ProtocolException(Protocol.ERR_BAD_REQUEST, "invalid filename");
        }

        Path candidate;
        try 
        {
            candidate = root.resolve(filename);
        } 
        catch (InvalidPathException e) 
        {
            throw new ProtocolException(Protocol.ERR_BAD_REQUEST, "invalid filename");
        }

        // ต้องเป็นไฟล์ปกติ (ไม่ใช่โฟลเดอร์ / device)
        if (!Files.isRegularFile(candidate)) 
        {
            throw notFound(filename);
        }

        // ชั้น 2: ตรวจปลายทางจริงหลังตาม symlink ต้องยังอยู่ใต้ root
        // ตอบ 404 เหมือนไม่พบไฟล์ ไม่บอกว่า "มีแต่ถูกห้าม" เพื่อไม่เปิดเผยว่ามีอะไรอยู่นอกโฟลเดอร์
        try 
        {
            Path real = candidate.toRealPath();
            if (!real.startsWith(root)) 
            {
                throw notFound(filename);
            }
            return real;
        } 
        catch (IOException e) 
        {
            throw notFound(filename);
        }
    }

    // ============================================================
    // ตัวช่วย
    // ============================================================

    private static ProtocolException notFound(String filename) 
    {
        return new ProtocolException(Protocol.ERR_NOT_FOUND, "file not found: " + filename);
    }

    private static boolean hasWhitespace(String s) 
    {
        for (int i = 0; i < s.length(); i++) 
        {
            if (Character.isWhitespace(s.charAt(i))) 
            {
                return true;
            }
        }
        return false;
    }
}
