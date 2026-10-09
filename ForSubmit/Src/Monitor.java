/**
 * Thread ที่รายงานสถานะระบบเป็นระยะ
 *
 * ===== ไฟล์นี้เป็นโครงเปล่า นักศึกษาต้องเขียนเอง =====
 *
 * ข้อกำหนดจากโจทย์ (หัวข้อ 10):
 *   - รายงานประมาณทุก 1,000 ms ไม่ต้องแม่นตรงทุกครั้ง
 *   - อย่างน้อยต้องมี ready, running, completed และสถานะการใช้ resource
 *   - ข้อมูลที่อ่านต้องเป็น snapshot ที่ปลอดภัย
 *     โดยเฉพาะตัวนับ running ซึ่ง Worker หลายตัวเพิ่ม/ลดพร้อมกัน
 *   - ห้ามอ่าน collection หรือตัวนับที่กำลังถูกแก้ไขโดยไม่มีการป้องกัน
 *
 * ให้พิมพ์ผ่าน logger.monitor(ready, running, completed, resources.status())
 * เพื่อให้รูปแบบตรงกับกลุ่มอื่น
 *
 * ข้อควรคิด: ตัวนับ running ควรอยู่ที่ไหน ใครเป็นคนเพิ่มและลด
 * และจะอ่านพร้อมกับ ready กับ completed ให้เป็นภาพเดียวกันได้อย่างไร
 * คำตอบ : ตัวนับอยู่ใน Statistics ซึ่งเพิ่มลดผ่าน Worker ในขณะที่ Monitor อ่านผ่าน statistics.snapshot() ทำให้เป็นภาพเดียวกัน
 */
public class Monitor extends Thread 
{

    // ---------- การออกแบบ ----------
    // ตัวนับ running อยู่ใน Statistics (Worker เป็นผู้เพิ่ม/ลด) จึงไม่ต้องเพิ่ม parameter ให้ constructor
    // Monitor อ่านผ่าน method ที่ป้องกันด้วยล็อกแล้วเท่านั้น ไม่แตะ collection หรือตัวนับตรง ๆ:
    //   - readyQueue.size()      : อ่านภายใต้ล็อกของ ReadyQueue
    //   - statistics.snapshot()  : running กับ completed ในภาพเดียวกัน (อ่านภายใต้ล็อกเดียว)
    //   - resources.status()     : อ่านจาก Semaphore.availablePermits()
    // ready อยู่คนละล็อกกับ running/completed จึงไม่ใช่ภาพชั่วขณะเดียวกันเป๊ะ ๆ
    // แต่ไม่เคยได้ค่าที่ผิดเชิงตรรกะ (ติดลบ หรือนับงานเดียวซ้ำสองสถานะใน Statistics)
    //
    // การหยุด: Main เรียก interrupt() แล้ว join() ซึ่ง sleep() จะ throw InterruptedException ทันที
    //   Monitor จึงตื่นและจบเองโดยไม่ต้องรอครบ 1,000 ms

    private static final long INTERVAL_MS = 1000;

    private final ReadyQueue readyQueue;
    private final ResourceManager resources;
    private final Statistics statistics;
    private final ProjectLogger logger;
    public Monitor(ReadyQueue readyQueue, ResourceManager resources, Statistics statistics, ProjectLogger logger)
    {
        super("monitor");
        this.readyQueue = readyQueue;
        this.resources = resources;
        this.statistics = statistics;
        this.logger = logger;
    }

    @Override
    public void run()
    {
        try
        {
            while (true)
            {
                report();
                Thread.sleep(INTERVAL_MS);   // พักตัว ไม่กิน CPU
            }
        }
        catch (InterruptedException e)
        {
            // ได้รับสัญญาณให้หยุด: ออกจาก run() ปกติ
            Thread.currentThread().interrupt();
        }
    }

    /** อ่านสถานะแล้วรายงานหนึ่งบรรทัด */
    private void report() 
    {
        int ready = readyQueue.size();
        Statistics.Snapshot snap = statistics.snapshot();
        logger.monitor(ready, snap.running, snap.completed, resources.status());
    }
}
