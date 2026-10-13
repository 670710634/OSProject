import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * คิวงานที่พร้อมถูกหยิบไปทำ
 *  - เก็บงานและ เลือกงานถัดไปตามนโยบาย (FCFS หรือ Priority)
 *  - ป้องกันไม่ให้หลาย Thread เข้าถึงพร้อมกันแล้วข้อมูลเพี้ยน
 *  - ให้ Worker ที่ไม่มีงาน พักตัวรอ โดยไม่วนลูปกิน CPU
 * ===== ไฟล์นี้เป็นโครงเปล่า นักศึกษาต้องเขียนเอง =====
 *
 * สิ่งที่คลาสนี้ต้องทำได้:
 *   - เก็บงานที่รอ Worker อยู่
 *   - หยิบงานถัดไปตามนโยบายที่เลือก (FCFS หรือ Priority)
 *   - ถูกเรียกจากหลาย Thread พร้อมกันได้อย่างปลอดภัย
 *
 * ข้อกำหนดจากโจทย์ที่เกี่ยวกับคลาสนี้:
 *   - หัวข้อ 4: priority = 1 สูงสุด เมื่อเท่ากันต้องมีกติกาตัดสินลำดับ (tie-break)
 *     ที่ตัดสินจากข้อมูลของ Job ไม่ขึ้นกับว่า Thread ใดเข้าถึงคิวก่อน
 *   - หัวข้อ 7: ห้ามวนลูปเช็กแบบกิน CPU (busy waiting) — Worker ที่ไม่มีงานทำ
 *     ต้องถูกพักไว้ ไม่ใช่วนถามซ้ำ ๆ
 *
 * จะออกแบบเป็นคลาสเดียวที่รับนโยบายเข้ามา หรือแยกเป็นสองคลาส
 * หรือใช้โครงสร้างข้อมูลสำเร็จรูปของ Java ก็ได้ ขอให้อธิบายเหตุผลได้ใน Demo
 */
public class ReadyQueue 
{

    /** งานหนึ่งชิ้นในคิว พร้อมลำดับที่เข้าคิว (ใช้กับ FCFS) */
    private static final class Entry 
    {
        final Job job;
        final long readyOrder;

        Entry(Job job, long readyOrder) 
        {
            this.job = job;
            this.readyOrder = readyOrder;
        }
    }

    private final Object lock = new Object();
    private final PriorityQueue<Entry> queue;   // เข้าถึงภายใต้ lock เท่านั้น
    private long nextReadyOrder = 0;            // เข้าถึงภายใต้ lock เท่านั้น
    private boolean closed = false;             // เข้าถึงภายใต้ lock เท่านั้น

    public ReadyQueue(Config.Policy policy) 
    {
        Comparator<Entry> comparator;
        switch (policy) 
        {
            case FCFS:
                comparator = Comparator.comparingLong((Entry e) -> e.readyOrder);
                break;

            case PRIORITY:
                comparator = Comparator.comparingInt((Entry e) -> e.job.priority)
                        .thenComparingLong(e -> e.job.arrivalMs)
                        .thenComparingInt(e -> e.job.sequence);
                break;

            default:
                throw new IllegalArgumentException("ไม่รู้จักนโยบาย: " + policy);
        }
        this.queue = new PriorityQueue<>(comparator);
    }

    /** ใส่งานเข้าคิว เรียกโดย Scheduler Thread */
    public void add(Job job)
    {
        synchronized (lock)
        {
            if (closed)
            {
                throw new IllegalStateException("ReadyQueue ถูกปิดแล้ว เพิ่มงาน " + job.id + " ไม่ได้");
            }
            queue.add(new Entry(job, nextReadyOrder++));
            lock.notify();   // ปลุก Worker ที่รออยู่ 1 ตัว (งานเข้า 1 ชิ้น)
        }
    }

    /**
     * หยิบงานถัดไปตามนโยบาย เรียกโดย Worker Thread
     *
     * ถ้ายังไม่มีงาน ต้องรอโดยไม่กิน CPU
     * ต้องคิดด้วยว่าจะบอก Worker อย่างไรเมื่อไม่มีงานเหลือแล้วและควรหยุดทำงาน
     */
    public Job take() throws InterruptedException 
    {
        // คืนค่า: งานถัดไปตามนโยบาย หรือ null เมื่อคิวถูกปิดและไม่มีงานเหลือแล้ว (Worker ควรหยุด)
        synchronized (lock) 
        {
            // ใช้ while ไม่ใช่ if: ตื่นแล้วต้องเช็กเงื่อนไขซ้ำ (กัน spurious wakeup และกรณีมี Worker อื่นแย่งงานไปก่อน)
            while (queue.isEmpty() && !closed) 
            {
                lock.wait();   // คืนล็อกและพักตัว ไม่กิน CPU
            }
            if (queue.isEmpty()) 
            {
                return null;   // closed และงานหมดแล้ว
            }
            return queue.poll().job;
        }
    }

    /** จำนวนงานที่รออยู่ตอนนี้ ใช้โดย Monitor — ต้องอ่านได้อย่างปลอดภัย */
    public int size() 
    {
        synchronized (lock) 
        {
            return queue.size();
        }
    }
    /**
     * บอกคิวว่า "จะไม่มีงานเข้ามาอีกแล้ว" เรียกโดย Scheduler หลังส่งงานครบ
     *
     * งานที่ค้างอยู่ในคิวยังถูกหยิบไปทำจนหมดตามปกติ เมื่อหมดแล้ว take() จะคืน null
     * เรียกซ้ำได้โดยไม่มีผลเพิ่ม
     */
    public void close() 
    {
        synchronized (lock) 
        {
            closed = true;
            lock.notifyAll();   // ปลุก Worker ทุกตัวที่รออยู่ ให้ออกจากการรอแล้วเช็กสถานะ
        }
    }
}
