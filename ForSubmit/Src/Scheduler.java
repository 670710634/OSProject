import java.util.concurrent.BlockingQueue;

/**
 * รับงานจาก JobGenerator แล้วจัดเข้า Ready Queue
 *
 * ===== ไฟล์นี้เป็นโครงเปล่า นักศึกษาต้องเขียนเอง =====
 *
 * ข้อกำหนดจากโจทย์ (หัวข้อ 2 และ 4):
 *   - Scheduler เป็น Thread บังคับ ห้ามให้ JobGenerator ใส่งานลง Ready Queue โดยตรง
 *   - รับผิดชอบการจัดลำดับตามนโยบาย FCFS หรือ Priority
 *
 * ข้อควรคิด:
 *   - Scheduler รับงานจาก JobGenerator ผ่านอะไร และรอโดยไม่กิน CPU อย่างไร
 *   - เมื่อ JobGenerator ปล่อยงานครบแล้ว Scheduler รู้ได้อย่างไรว่าควรหยุด
 */
public class Scheduler extends Thread 
{

    // รับงานจาก arrivalQueue (ที่ JobGenerator ส่งมา) แล้วใส่ ReadyQueue
    // ลำดับของงานที่ Worker จะได้รับ ReadyQueue เป็นผู้จัดตามนโยบาย (FCFS/Priority)
    // Scheduler มีหน้าที่ "ส่งต่อ" และ "ปิดคิว" เมื่อไม่มีงานเข้ามาอีก
    private final BlockingQueue<Job> arrivalQueue;
    private final ReadyQueue readyQueue;
    private final ProjectLogger logger;

    public Scheduler(BlockingQueue<Job> arrivalQueue, ReadyQueue readyQueue, ProjectLogger logger)
    {
        super("scheduler");
        this.arrivalQueue = arrivalQueue;
        this.readyQueue = readyQueue;
        this.logger = logger;
    }

    @Override
    public void run() 
    {
        int dispatched = 0;
        try
        {
            while (true)
            {
                Job job = arrivalQueue.take();   // รอโดยไม่กิน CPU
                if (job == JobGenerator.END_OF_ARRIVALS) 
                {
                    break;                       // ไม่มีงานเข้ามาอีกแล้ว
                }
                readyQueue.add(job);
                dispatched++;
            }
        } 
        catch (InterruptedException e) 
        {
            logger.systemEvent("scheduler interrupted - stopping early");
            Thread.currentThread().interrupt();
        } 
        finally 
        {
            // ปิด ReadyQueue เสมอ เพื่อปลุก Worker ที่รออยู่ให้จบเอง (งานที่ค้างในคิวยังถูกทำจนหมด)
            readyQueue.close();
            logger.systemEvent("scheduler dispatched " + dispatched + " jobs, ready queue closed");
        }
    }
}
