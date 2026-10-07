import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * ปล่อยงานเข้าสู่ระบบตามเวลา arrivalMs ของแต่ละ Job
 *
 * ===== ไฟล์นี้เป็นโครงเปล่า นักศึกษาต้องเขียนเอง =====
 *
 * หน้าที่ (หัวข้อ 3 ของโจทย์):
 *   - รอจนถึงเวลา arrivalMs ของแต่ละงาน แล้วส่งงานต่อไปยัง Scheduler
 *   - บันทึกเวลาที่งานเข้าสู่ระบบ "จริง" ลงใน Job
 *     (อาจไม่ตรงกับ arrivalMs เป๊ะ เพราะ Thread ถูกปลุกช้าได้)
 *   - เรียก logger.jobArrived(job) ทุกครั้งที่ปล่อยงาน
 *
 * ข้อควรคิด:
 *   - รายการงานที่ได้จาก WorkloadLoader เรียงตามลำดับในไฟล์ ไม่ได้เรียงตามเวลา
 *   - เมื่อปล่อยงานครบทุกชิ้นแล้ว ต้องมีวิธีบอกระบบว่า "จะไม่มีงานเข้ามาอีก"
 *     ดู TODO เรื่องการปิดระบบใน Main
 */
public class JobGenerator extends Thread 
{

    // ---------- การออกแบบ ----------
    // ช่องทางส่งงานไป Scheduler คือ BlockingQueue<Job> ที่เรียกว่า arrivalQueue (ตามแผนผังของโจทย์)
    //   - JobGenerator เป็นผู้ส่ง (Thread เดียว), Scheduler เป็นผู้รับ (Thread เดียว)
    //   - Scheduler รอด้วย arrivalQueue.take() จึงไม่กิน CPU
    //
    // สัญญาณ "ไม่มีงานเข้ามาอีกแล้ว": poison pill
    //   JobGenerator ส่ง END_OF_ARRIVALS เป็นชิ้นสุดท้าย Scheduler เห็นแล้วหยุดและปิด ReadyQueue
    //   ใช้ poison pill ได้เพราะที่นี่มีผู้รับตัวเดียว (ต่างจาก Worker ที่มีหลายตัว จึงใช้ close() แทน)
    //
    // การรอเวลา: ใช้เวลาสัมบูรณ์จาก logger.now() (คิดจาก arrivalMs ของแต่ละงาน)
    //   ไม่ใช้ sleep แบบสะสมช่วงห่าง เพราะความคลาดเคลื่อนจะสะสมไปเรื่อย ๆ (drift)

    /** งานพิเศษที่ใช้เป็นสัญญาณจบ (poison pill) ไม่ใช่งานจริง ห้ามถูกทำโดย Worker */
    public static final Job END_OF_ARRIVALS = new Job("__END__", Long.MAX_VALUE, Integer.MAX_VALUE, 0, ResourceType.NONE, 0, Integer.MAX_VALUE);

    private final List<Job> jobs;                    // สำเนาที่เรียงตามเวลาเข้าแล้ว
    private final BlockingQueue<Job> arrivalQueue;
    private final ProjectLogger logger;

    public JobGenerator(List<Job> jobs, BlockingQueue<Job> arrivalQueue, ProjectLogger logger)
    {
        super("generator");

        // รายการจาก WorkloadLoader แก้ไขไม่ได้และเรียงตามลำดับในไฟล์ จึงคัดลอกแล้วเรียงตามเวลาเข้า
        // เวลาเข้าเท่ากันให้ใช้ลำดับในไฟล์ (sequence) ตัดสิน ผลจึงแน่นอนทุกครั้ง

        List<Job> sorted = new ArrayList<>(jobs);
        sorted.sort(Comparator.comparingLong((Job j) -> j.arrivalMs).thenComparingInt(j -> j.sequence));
        this.jobs = sorted;
        this.arrivalQueue = arrivalQueue;
        this.logger = logger;
    }
    @Override
    public void run()
    {
        try
        {
            for (Job job : jobs)
            {
                // รอจนถึงเวลา arrivalMs (นับจากเวลาเริ่มระบบ = logger.now() เท่ากับ 0)
                long waitMs = job.arrivalMs - logger.now();
                if (waitMs > 0)
                {
                    Thread.sleep(waitMs);
                }

                // บันทึกเวลาเข้าจริงก่อน แล้วค่อย log และส่งต่อ (Worker จะอ่านค่านี้ทีหลัง)
                job.markArrived(logger.now());
                logger.jobArrived(job);
                arrivalQueue.put(job);   // ผ่าน Scheduler เสมอ ไม่ใส่ ReadyQueue โดยตรง
            }
        }
        catch (InterruptedException e)
        {
            logger.systemEvent("generator interrupted - stopping early");
            Thread.currentThread().interrupt();
        }
        finally
        {
            // ส่งสัญญาณจบเสมอ แม้ถูก interrupt เพื่อให้ Scheduler ไม่รอตลอดไป
            // arrivalQueue ไม่จำกัดขนาด offer จึงสำเร็จทันทีและไม่ถูกบล็อก
            arrivalQueue.offer(END_OF_ARRIVALS);
        }
    }
}
