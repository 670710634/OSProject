/**
 * Thread ที่ดึงงานจาก Ready Queue ไปทำจนเสร็จ
 *
 * ===== ไฟล์นี้เป็นโครงเปล่า นักศึกษาต้องเขียนเอง =====
 *
 * ลำดับการทำงานของ Job หนึ่งชิ้น บังคับตามหัวข้อ 6 ของโจทย์:
 *   1. รับงานจาก Ready Queue แล้วบันทึกเวลาเริ่ม
 *   2. จำลองงานหลักด้วย Thread.sleep(job.workMs)
 *   3. ถ้า job.resource != NONE ให้บันทึกเวลาเริ่มรอ แล้ว acquire
 *   4. จำลองการถือครองด้วย Thread.sleep(job.resourceMs)
 *   5. release แล้วบันทึกเวลาจบ
 *
 * ห้ามสลับขั้นที่ 2 กับ 3 เพราะจะทำให้ผลของทุกกลุ่มเทียบกันไม่ได้
 *
 * จุดที่มักพลาด:
 *   - ถ้า exception หรือ interrupt เกิดขึ้นหลัง acquire แต่ก่อน release
 *     permit จะค้างถาวรและระบบจะแขวน ต้องออกแบบให้คืนได้เสมอ
 *   - Worker ต้องหยุดเองได้เมื่อไม่มีงานเหลือแล้ว ไม่ใช่วนรอตลอดไป
 */
public class Worker extends Thread 
{

    // ทุก Worker ใช้ ReadyQueue, ResourceManager, Statistics และ logger "ตัวเดียวกัน" ทั้งระบบ
    // (Main สร้างครั้งเดียวแล้วส่งให้ทุกตัว) ฟิลด์เป็น final ถูกกำหนดครั้งเดียวตอนสร้าง

    private final ReadyQueue readyQueue;
    private final ResourceManager resources;
    private final Statistics statistics;
    private final ProjectLogger logger;

    public Worker(String name, ReadyQueue readyQueue, ResourceManager resources, Statistics statistics, ProjectLogger logger) 
    {
        super(name);
        this.readyQueue = readyQueue;
        this.resources = resources;
        this.statistics = statistics;
        this.logger = logger;
    }

    @Override
    public void run() {
        // วงจรชีวิตของ Worker: รับงาน -> ทำ -> รับงานถัดไป ... จนได้ null
        // take() คืน null เมื่อคิวถูกปิด (close) และไม่มีงานเหลือแล้ว นั่นคือสัญญาณให้หยุดเอง
        // ระหว่างที่ทำงานชิ้นสุดท้ายอยู่ Worker จะไม่ถูกตัดกลางคัน เพราะเช็ก null เฉพาะตอนรับงานใหม่
        try
        {
            Job job;
            while ((job = readyQueue.take()) != null)
            {
                processJob(job);
            }
        }
        catch (InterruptedException e)
        {
            // ถูก interrupt ระหว่างรอรับงาน หรือระหว่างทำงาน (processJob คืน permit ให้แล้วใน finally)
            // ถือว่าเป็นคำสั่งให้หยุด: คืนค่า interrupt flag แล้วออกจาก run()
            logger.systemEvent("interrupted - worker stopping");
            Thread.currentThread().interrupt();
        }
    }

    /** ทำงานหนึ่งชิ้นให้จบตามลำดับ 5 ขั้นด้านบน */
    private void processJob(Job job) throws InterruptedException {
        // ขั้นที่ 1: รับงานและบันทึกเวลาเริ่ม (นาฬิกาเดียวกับ log: logger.now())
        statistics.recordStart();                 // running++ (Monitor เห็นทันที)
        boolean completedOk = false;
        try
        {
            job.markStarted(logger.now());
            logger.jobStarted(job);

            // ขั้นที่ 2: งานหลัก
            Thread.sleep(job.workMs);
            logger.workFinished(job);

            // ขั้นที่ 3-6: ใช้ทรัพยากรร่วม (ถ้ามี)
            if (job.resource != ResourceType.NONE) 
            {
                useResource(job);
            }

            // ขั้นที่ 7: บันทึกเวลาจบและสถิติ
            job.markCompleted(logger.now());
            logger.jobCompleted(job);
            statistics.recordCompletion(job);     // running-- และ completed++ ในล็อกเดียว
            completedOk = true;
        } 
        finally 
        {
            // ถ้าไม่เสร็จปกติ (ถูก interrupt หรือเกิด exception) ต้องลด running ไม่ให้ค้าง
            if (!completedOk) 
            {
                statistics.recordAbort();
            }
        }
    }

    /** ขอ resource -> ถือครอง -> คืน รับประกันว่าคืน permit เสมอ */
    private void useResource(Job job) throws InterruptedException 
    {
        ResourceType type = job.resource;
 
        job.markResourceWaitStarted(logger.now());
        logger.resourceWaitStarted(job);
 
        // acquire "ต้องอยู่นอก try":
        // ถ้าถูก interrupt ระหว่างรอ acquire จะ throw ก่อนได้ permit
        // ถ้าอยู่ใน try แล้วไป release ใน finally จะคืน permit ที่ไม่เคยได้ (permit เกินของจริง)
        resources.acquire(type);
 
        // try เริ่มทันทีหลัง acquire ไม่มีโค้ดอื่นคั่นกลาง เพื่อไม่ให้มีช่องที่ได้ permit แล้วไม่ได้คืน
        try
        {
            long waited = job.markResourceAcquired(logger.now());
            logger.resourceAcquired(job, waited);
 
            Thread.sleep(job.resourceMs);         // ตรงนี้ถูก interrupt ได้ finally จะคืนให้
        } 
        finally
        {
            try
            {
                // log RELEASED "ก่อน" release จริง เพื่อให้ log ไม่เคยแสดงว่ามี 2 งานถือพร้อมกัน
                // (ถ้า release ก่อนแล้วค่อย log อาจเห็น ACQUIRED ของงานอื่นมาก่อนบรรทัด RELEASED)
                logger.resourceReleased(job);
            } 
            finally 
            {
                resources.release(type);          // ถูกเรียกเสมอ แม้ log มีปัญหา
            }
        }
    }
}
