import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * จุดเริ่มต้นของโปรแกรม
 *
 * ===== ไฟล์นี้เป็นโครงเปล่า นักศึกษาต้องเขียนเอง =====
 *
 * ส่วนที่เขียนไว้ให้แล้วคือการรับค่า การโหลด workload และการแสดง error
 * ซึ่งไม่ใช่สิ่งที่โครงงานนี้วัด ส่วนที่เหลือเป็น TODO ทั้งหมด
 *
 * วิธีรัน:
 *   java Main jobs_standard.csv priority 3 1 2
 */
public class Main 
{

    public static void main(String[] args) 
    {
        // ---------- 1. รับค่าจาก command line ----------
        Config config;
        try
        {
            config = Config.parse(args);
        }
        catch (IllegalArgumentException e)
        {
            System.err.println("ผิดพลาด: " + e.getMessage());
            System.err.println();
            System.err.println(Config.USAGE);
            System.exit(1);
            return;
        }

        // ---------- 2. เริ่มจับเวลาและโหลด workload ----------
        ProjectLogger logger = new ProjectLogger();
        List<Job> jobs;
        try
        {
            jobs = WorkloadLoader.load(config.workloadPath);
        }
        catch (WorkloadFormatException e)
        {
            System.err.println("ไฟล์ workload ผิดรูปแบบ — " + e.getMessage());
            System.exit(1);
            return;
        }
        catch (java.io.IOException e)
        {
            System.err.println("เปิดไฟล์ \"" + config.workloadPath + "\" ไม่ได้");
            System.err.println("ตรวจว่าไฟล์มีอยู่จริงและ path ถูกต้อง (สั่ง java จากโฟลเดอร์ใด)");
            System.exit(1);
            return;
        }
        logger.systemStart(config);
        logger.systemEvent("โหลดงานได้ " + jobs.size() + " ชิ้น");

        // ---------- 3. สร้างส่วนประกอบของระบบ ----------
        // ทุกส่วนประกอบถูกสร้าง "ครั้งเดียว" แล้วส่งให้ทุก Thread ใช้ร่วมกัน (Worker ทุกตัวใช้ resources ตัวเดียวกัน)
        ResourceManager resources = new ResourceManager(config.printerPermits, config.databasePermits);
        ReadyQueue readyQueue = new ReadyQueue(config.policy);
        Statistics statistics = new Statistics();
        // ช่องทางส่งงานจาก JobGenerator ไป Scheduler (ห้ามใส่ ReadyQueue ตรง ๆ)
        BlockingQueue<Job> arrivalQueue = new LinkedBlockingQueue<>();

        // ---------- 4. สร้างและเริ่ม Thread ----------
        // ลำดับ start: ฝั่งผู้รับก่อน (Worker -> Scheduler -> Monitor) และ JobGenerator เป็นตัวสุดท้าย
        // เพราะ generator คือตัวที่ "เริ่มจ่ายงาน" ถ้าผู้รับยังไม่พร้อม งานอาจเข้าคิวรอโดยที่ยังไม่มีใครหยิบ
        // ทำให้ Waiting Time ของงานแรก ๆ สูงเกินจริง (ระบบยังถูกต้อง เพราะคิวเก็บงานไว้ให้ แต่ผลวัดเพี้ยน)
        List<Worker> workers = new ArrayList<>();
        for (int i = 1; i <= config.workers; i++) 
        {
            Worker worker = new Worker("worker-" + i, readyQueue, resources, statistics, logger);
            workers.add(worker);
            worker.start();
        }
        Scheduler scheduler = new Scheduler(arrivalQueue, readyQueue, logger);
        scheduler.start();
        Monitor monitor = new Monitor(readyQueue, resources, statistics, logger);
        monitor.start();
        JobGenerator generator = new JobGenerator(jobs, arrivalQueue, logger);
        generator.start();

        // ---------- 5. รอจนงานเสร็จครบ ----------
        // TODO: รอจนกว่างานทั้ง jobs.size() ชิ้นจะเสร็จ
        //
        // *** นี่คือจุดที่ยากที่สุดของโครงงานนี้ ***
        // Worker ที่กำลังรออยู่ในคิวไม่มีทางรู้ได้เองว่าจะไม่มีงานเข้ามาอีกแล้ว
        // กลุ่มต้องออกแบบวิธีบอก โดยห้ามใช้การเดาเวลา เช่น sleep(10000)
        //
        // เทคนิคที่ไปหาอ่านต่อได้ (เลือกใช้อันใดอันหนึ่งหรือผสมกันก็ได้):
        //   - poison pill
        //   - CountDownLatch
        //   - ตัวนับงานค้างที่ป้องกันด้วย lock
        //
        // อาการผิดที่ต้องไม่เกิด:
        //   1. main จบแล้วแต่ JVM ไม่ปิด เพราะยังมี Thread ค้างอยู่
        //   2. Worker หยุดก่อนที่งานชิ้นสุดท้ายจะทำเสร็จ
        //   3. permit ค้างเพราะถูก interrupt ระหว่างถือ resource

        // วิธีที่เลือก: ปล่อยให้สัญญาณจบไหลไปตามสายโซ่ แล้ว main แค่ join ตามลำดับ (ไม่เดาเวลา)
        //   generator ส่งงานครบ -> ส่ง poison pill ให้ Scheduler -> Scheduler close() ReadyQueue
        //   -> Worker ทำงานที่ค้างจนหมดแล้ว take() ได้ null -> Worker จบเอง
        // main รอการจบของแต่ละ Thread ด้วย join() ซึ่งหมายความว่า "งานทุกชิ้นเสร็จแล้ว"
        // (Worker จะจบได้ก็ต่อเมื่อคิวปิดและว่าง และงานที่กำลังทำอยู่เสร็จสมบูรณ์แล้ว)
        boolean mainInterrupted = false;
        try
        {
            generator.join();
            scheduler.join();
            for (Worker worker : workers)
            {
                worker.join();
            }
        }
        catch (InterruptedException e)
        {
            // กรณีฉุกเฉิน: main ถูกสั่งหยุดก่อนงานเสร็จ ให้หยุดทุก Thread แล้วไปสรุปผลเท่าที่มี
            mainInterrupted = true;
            logger.systemEvent("main interrupted - stopping all threads");
            generator.interrupt();
            scheduler.interrupt();
            for (Worker worker : workers)
            {
                worker.interrupt();
            }
        }

        // ---------- 6. สั่งหยุดทุก Thread ----------
        // Worker, Scheduler และ JobGenerator จบเองแล้ว (หรือถูก interrupt ในกรณีฉุกเฉินข้างบน)
        // เหลือ Monitor ที่วนอยู่ตลอด จึงสั่งหยุดด้วย interrupt() ซึ่งปลุกจาก sleep ทันที
        monitor.interrupt();
        // join ทุก Thread เพื่อยืนยันว่าหยุดจริง ก่อนอ่านผลสรุป
        joinAll(generator, scheduler, monitor);
        for (Worker worker : workers) 
        {
            joinAll(worker);
        }
        if (mainInterrupted) 
        {
            Thread.currentThread().interrupt();
        }

        // ---------- 7. สรุปผล ----------
        // makespan = เวลาที่งานชิ้นสุดท้ายเสร็จ (นับจาก simulationStart ด้วยนาฬิกาเดียวกับ logger.now())
        long makespanMs = 0;
        for (Job job : jobs) 
        {
            if (job.isMeasured()) 
            {
                makespanMs = Math.max(makespanMs, job.getCompletionMs());
            }
        }
        if (makespanMs == 0) 
        {
            makespanMs = logger.now();   // ไม่มีงานใดเสร็จเลย
        }
        int completed = statistics.completedCount();
        statistics.printSummary(jobs, makespanMs);
        logger.systemStop(completed, jobs.size());
        // ไม่เรียก System.exit(): ทุก Thread จบแล้ว JVM จึงปิดเองเมื่อ main จบ
    }

        /** join Thread ให้จบจริง แม้ main ถูก interrupt ระหว่างรอ (จดจำไว้แล้วรอต่อ ไม่ทิ้ง Thread ค้าง) */
    private static void joinAll(Thread... threads)
    {
        boolean interrupted = false;
        for (Thread thread : threads)
        {
            while (true)
            {
                try
                {
                    thread.join();
                    break;
                }
                catch (InterruptedException e)
                {
                    interrupted = true;
                }
            }
        }
        if (interrupted)
        {
            Thread.currentThread().interrupt();
        }
    }
}
