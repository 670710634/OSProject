import java.util.List;
import java.util.Locale;

/**
 * รวบรวมและคำนวณค่าที่ใช้วัดผลของการรันหนึ่งครั้ง
 *  - นับสถานะสด (กำลังทำกี่งาน เสร็จกี่งาน) ให้ Monitor อ่านระหว่างรัน
 *  - เก็บผลรวมเวลา แล้วคำนวณค่าเฉลี่ยและ Throughput พิมพ์เป็นตารางสรุปตอนจบ
 * ===== ไฟล์นี้เป็นโครงเปล่า นักศึกษาต้องเขียนเอง =====
 *
 * ข้อกำหนดจากโจทย์ที่เกี่ยวกับคลาสนี้ (หัวข้อ 8):
 *   - Waiting Time, Turnaround Time, Throughput, Resource Wait Time
 *   - ต้องถูกอัปเดตจากหลาย Worker พร้อมกันได้อย่างปลอดภัย
 *   - ผลต้องสอดคล้องกับสมการตรวจสอบ:
 *       Turnaround = Waiting + workMs + Resource Wait + resourceMs
 *     ใช้สมการนี้ตรวจงานทีละชิ้นได้ว่าค่าไหนคำนวณผิด
 *
 * ข้อควรระวัง: ค่าเฉลี่ยของ Resource Wait ให้คิดเฉพาะงานที่ใช้ resource
 * ส่วนงานที่ resource = NONE ให้ถือว่า Resource Wait เป็น 0
 */
public class Statistics 
{

    // ---------- การออกแบบ ----------
    // เก็บ "ผลรวม" ไว้ (ไม่เก็บรายการงานทั้งหมด) แล้วหารตอนพิมพ์สรุป
    // ตัวนับทั้งหมดอยู่ที่นี่ รวมถึง running เพราะ Monitor ได้รับ Statistics อยู่แล้ว
    //   - Worker เรียก recordStart() ตอนรับงาน -> running++
    //   - Worker เรียก recordCompletion(job) ตอนงานเสร็จ -> running-- และ completed++ ในล็อกเดียวกัน
    //     ทำให้ Monitor ไม่มีทางเห็นงานที่ "ไม่นับเป็นทั้ง running และ completed" หรือ "นับซ้ำสองที่"
    //   - ถ้า Worker ถูก interrupt กลางงาน เรียก recordAbort() เพื่อลด running ไม่ให้ค้าง
    //
    // ป้องกันด้วย synchronized (lock) ตัวเดียว: ตัวนับและผลรวมทุกตัวถูกแก้/อ่านภายใต้ล็อกนี้เท่านั้น
 
    private final Object lock = new Object();
    private int running = 0;
    private int completed = 0;
    private long sumWaitingMs = 0;
    private long sumTurnaroundMs = 0;
    private long sumResourceWaitMs = 0;   // รวมเฉพาะงานที่ใช้ resource
    private int resourceJobCount = 0;     // จำนวนงานที่ใช้ resource (ตัวหารของ avg Resource Wait)

    /** ภาพของ running และ completed ณ ขณะเดียวกัน (อ่านภายใต้ล็อกเดียว) */
    public static final class Snapshot 
    {
        public final int running;
        public final int completed;

        Snapshot(int running, int completed) 
        {
            this.running = running;
            this.completed = completed;
        }
    }

    /** Worker เรียกตอนรับงานไปเริ่มทำ */
    public void recordStart() 
    {
        synchronized (lock) 
        {
            running++;
        }
    }

    /** Worker เรียกเมื่องานถูกยกเลิกกลางทาง (เช่น ถูก interrupt) เพื่อไม่ให้ running ค้าง */
    public void recordAbort() 
    {
        synchronized (lock) 
        {
            if (running <= 0) 
            {
                throw new IllegalStateException("recordAbort() โดยไม่มีงานที่กำลังทำอยู่");
            }
            running--;
        }
    }

    /** จำนวนงานที่กำลังทำอยู่ ใช้โดย Monitor */
    public int runningCount() 
    {
        synchronized (lock) 
        {
            return running;
        }
    }

    /** running และ completed ในภาพเดียวกัน ใช้โดย Monitor */
    public Snapshot snapshot() 
    {
        synchronized (lock) 
        {
            return new Snapshot(running, completed);
        }
    }

    /** บันทึกว่างานชิ้นหนึ่งเสร็จแล้ว เรียกโดย Worker หลายตัวพร้อมกันได้ */
    public void recordCompletion(Job job) 
    {
        if (!job.isMeasured()) 
        {
            throw new IllegalStateException("งาน " + job.id + " บันทึกเวลายังไม่ครบ (arrival/start/completion)");
        }
        // อ่านค่าจาก Job นอกล็อก (ฟิลด์เป็น volatile และงานนี้ Worker ตัวเดียวถืออยู่) เพื่อถือล็อกให้สั้นที่สุด
        long wt = job.waitingMs();
        long tat = job.turnaroundMs();
        long rw = job.getResourceWaitMs();

        synchronized (lock) 
        {
            if (running <= 0) 
            {
                throw new IllegalStateException("recordCompletion(" + job.id + ") โดยไม่ได้ recordStart() ก่อน");
            }
            running--;
            completed++;
            sumWaitingMs += wt;
            sumTurnaroundMs += tat;
            if (job.resource != ResourceType.NONE) 
            {
                resourceJobCount++;
                sumResourceWaitMs += rw;
            }
        }
    }

    /** จำนวนงานที่เสร็จแล้ว ใช้โดย Monitor และใช้ตรวจว่างานครบหรือยัง */
    public int completedCount()
    {
        synchronized (lock)
        {
            return completed;
        }
    }

    /**
     * พิมพ์ตารางสรุปผลตอนจบโปรแกรม
     * อย่างน้อยต้องมี avg Waiting Time, avg Turnaround Time,
     * Throughput และ avg Resource Wait Time
     *
     * ตามหัวข้อ 14 ให้รายงานเวลาเป็นจำนวนเต็มหน่วย ms
     * และ Throughput อย่างน้อย 2 ตำแหน่งทศนิยม
     */
    public void printSummary(List<Job> allJobs, long makespanMs) 
    {
        // คัดลอกค่าภายใต้ล็อกครั้งเดียว แล้วคำนวณและพิมพ์นอกล็อก
        int done;
        long sumWt, sumTat, sumRw;
        int rwJobs;
        synchronized (lock) 
        {
            done = completed;
            sumWt = sumWaitingMs;
            sumTat = sumTurnaroundMs;
            sumRw = sumResourceWaitMs;
            rwJobs = resourceJobCount;
        }

        // ----- ตารางรายงาน: ใช้ตรวจสมการ TAT = WT + workMs + RW + resourceMs ทีละชิ้น -----
        // gap = TAT จริง - ผลรวมตามสมการ ควรใกล้ 0 (คลาดเล็กน้อยจาก sleep ที่หลับเกินเวลา
        // และการตัดเศษ ms) ถ้า gap ใหญ่ผิดปกติ แปลว่าบันทึกเวลาผิดจุด
        System.out.println();
        System.out.println("===== PER-JOB TABLE (ms) =====");
        System.out.println(String.format(Locale.ROOT, "%-6s %4s %7s %7s %7s %7s %7s %7s %7s %7s","job", "pri", "WT", "work", "RW", "resMs", "TAT", "gap", "start", "done"));
        for (Job job : allJobs) 
            {
            if (!job.isMeasured()) 
            {
                System.out.println(String.format(Locale.ROOT, "%-6s %4d  NOT COMPLETED", job.id, job.priority));
                continue;
            }
            long wt = job.waitingMs();
            long tat = job.turnaroundMs();
            long rw = job.getResourceWaitMs();
            long gap = tat - (wt + job.workMs + rw + job.resourceMs);
            System.out.println(String.format(Locale.ROOT, "%-6s %4d %7d %7d %7d %7d %7d %7d %7d %7d", job.id, job.priority, wt, job.workMs, rw, job.resourceMs, tat, gap, job.getStartMs(), job.getCompletionMs()));
        }

        // ----- สรุป -----
        long avgWt = done == 0 ? 0 : Math.round((double) sumWt / done);
        long avgTat = done == 0 ? 0 : Math.round((double) sumTat / done);
        // avg Resource Wait คิดเฉพาะงานที่ใช้ PRINTER/DATABASE ตามโจทย์
        long avgRw = rwJobs == 0 ? 0 : Math.round((double) sumRw / rwJobs);
        double throughput = makespanMs <= 0 ? 0.0 : done / (makespanMs / 1000.0);
 
        System.out.println();
        System.out.println("===== SUMMARY =====");
        System.out.println(String.format(Locale.ROOT, "completed jobs      : %d/%d", done, allJobs.size()));
        System.out.println(String.format(Locale.ROOT, "total time          : %d ms", makespanMs));
        System.out.println(String.format(Locale.ROOT, "avg Waiting Time    : %d ms", avgWt));
        System.out.println(String.format(Locale.ROOT, "avg Turnaround Time : %d ms", avgTat));
        System.out.println(String.format(Locale.ROOT, "avg Resource Wait   : %d ms  (over %d jobs using a resource)", avgRw, rwJobs));
        System.out.println(String.format(Locale.ROOT, "Throughput          : %.2f jobs/s", throughput));
    }
}

