import java.util.concurrent.Semaphore;

/** 
 * ควบคุมสิทธิ์การใช้ทรัพยากรร่วมของทั้งระบบ
 *
 * ===== ไฟล์นี้เป็นโครงเปล่า นักศึกษาต้องเขียนเอง =====
 *
 * ข้อกำหนดจากโจทย์ที่เกี่ยวกับคลาสนี้:
 *   - หัวข้อ 5: ใช้ Semaphore ควบคุม PRINTER และ DATABASE
 *     จำนวน permit มาจาก command line (Config)
 *     ในส่วนบังคับให้สร้าง Semaphore แบบ fair = true
 *   - Worker ทุกตัวต้องใช้ ResourceManager object เดียวกัน
 *   - หัวข้อ 7: permit ต้องไม่สูญหายหรือค้าง แม้เกิด exception
 *     หรือถูก interrupt ระหว่างถือ resource
 *
 * คำถามที่จะถูกถามใน Demo:
 *   - ทำไมต้อง fair = true และถ้าเปลี่ยนเป็น false จะเกิดอะไรขึ้น
 *   - ถ้า Thread ถูก interrupt หลัง acquire สำเร็จแต่ก่อน release
 *     โค้ดของกลุ่มยังคืน permit ได้หรือไม่
 * คำตอบ
 *   - บังคับให้ Thread ที่มาก่อนได้ทำก่อนแบบ FIFO ถ้าเป็น False อาจทำให้เกิดการแซงคิวได้ก่อ Starvation ได้
 *   - โค้ดของกลุ่มยังคืน permit ได้ หากมีการป้องกันอย่างถูกต้องใน Worker.java ตามที่เอกสารเตือนไว้
**/

public class ResourceManager
{
    // ทุกฟิลด์เป็น final ถูกกำหนดครั้งเดียวตอนสร้าง จึงอ่านจากหลาย Thread ได้ปลอดภัย
    private final Semaphore printer;
    private final Semaphore database;

    // จำนวน permit ทั้งหมด เก็บไว้ใช้ใน status() (Semaphore ไม่มี method บอกจำนวนเริ่มต้น)
    private final int printerTotal;
    private final int databaseTotal;

    public ResourceManager(int printerPermits, int databasePermits)
    {
        this.printerTotal = printerPermits;
        this.databaseTotal = databasePermits;
        this.printer = new Semaphore(printerPermits, true);
        this.database = new Semaphore(databasePermits, true);
    }

    /* ขอสิทธิ์ใช้ทรัพยากร จะรอจนกว่าจะได้ */
    public void acquire(ResourceType type) throws InterruptedException
    {
        Semaphore s = semaphoreOf(type);
        if (s != null)
        {
            // ถ้าถูก interrupt ระหว่างรอ จะ throw InterruptedException และ "ยังไม่ได้" permit
            // ผู้เรียกจึงต้องไม่ release ในกรณีนี้ (ให้เรียก acquire ก่อนเข้า try ที่มี finally release)
            s.acquire();
        }
    }

    /** คืนสิทธิ์ใช้ทรัพยากร */
    public void release(ResourceType type) 
    {
        Semaphore s = semaphoreOf(type);
        if (s != null) 
        {
            s.release();
        }
    }

    /**
     * ข้อความสั้น ๆ บอกสถานะการใช้ทรัพยากร สำหรับส่งให้ ProjectLogger.monitor()
     * เช่น "printer=1/1 database=0/2"
     */
    public String status()
    {
        // รูปแบบ ใช้อยู่/ทั้งหมด เช่น printer=1/1 แปลว่าถูกใช้อยู่ 1 จาก 1 permit
        // availablePermits() อ่านได้ปลอดภัยจากหลาย Thread แต่เป็นภาพ ณ ขณะที่อ่าน
        // (printer กับ database อ่านแยกกัน จึงไม่ใช่ snapshot ชั่วขณะเดียวกันเป๊ะ ๆ)
        int printerUsed = printerTotal - printer.availablePermits();
        int databaseUsed = databaseTotal - database.availablePermits();
        return "printer=" + printerUsed + "/" + printerTotal + " database=" + databaseUsed + "/" + databaseTotal;
    }
    /** เลือก Semaphore ตามชนิดทรัพยากร คืน null เมื่อเป็น NONE (ไม่ต้องใช้ทรัพยากรร่วม) */
    private Semaphore semaphoreOf(ResourceType type)
    {
        switch (type) 
        {
            case PRINTER:
                return printer;
            case DATABASE:
                return database;
            case NONE:
                return null;
            default:
                throw new IllegalArgumentException("ไม่รู้จักชนิดทรัพยากร: " + type);
        }
    }
}
