/**
 * ข้อมูลของงานหนึ่งชิ้น
 *
 * ฟิลด์ทั้งหมดในไฟล์นี้มาจากไฟล์ workload CSV โดยตรง และถูกกำหนดครั้งเดียว
 * ตอนโหลด จึงประกาศเป็น final และปลอดภัยเมื่อหลาย Thread อ่านพร้อมกัน
 *
 * ไฟล์นี้เป็นโค้ดตั้งต้นที่อาจารย์แจก แต่ต่างจากไฟล์อื่นตรงที่
 * นักศึกษา "ต้องแก้" โดยเพิ่มฟิลด์ของตัวเองในส่วน TODO ด้านล่าง
 */
public class Job 
{

    /** รหัสงาน เช่น J01 — ไม่ซ้ำกันภายในหนึ่งไฟล์ workload */
    public final String id;

    /** เวลาที่งานควรเข้าสู่ระบบ นับจากวินาทีที่โปรแกรมเริ่ม (มิลลิวินาที) */
    public final long arrivalMs;

    /** ระดับความสำคัญ โดย 1 คือสูงสุด ตัวเลขยิ่งมากยิ่งสำคัญน้อย */
    public final int priority;

    /** ระยะเวลาของงานหลัก ก่อนขอใช้ทรัพยากรร่วม (มิลลิวินาที) */
    public final long workMs;

    /** ทรัพยากรร่วมที่ต้องใช้ หรือ NONE ถ้าไม่ต้องใช้ */
    public final ResourceType resource;

    /** ระยะเวลาที่ถือครองทรัพยากร (มิลลิวินาที) เป็น 0 เสมอเมื่อ resource เป็น NONE */
    public final long resourceMs;

    /**
     * ลำดับที่งานนี้ปรากฏในไฟล์ workload เริ่มจาก 0
     * เตรียมไว้ให้เผื่อกลุ่มต้องการใช้ประกอบการตัดสินลำดับเมื่อ priority เท่ากัน
     * จะใช้หรือไม่ใช้ก็ได้ กติกาตัดสินลำดับเป็นสิ่งที่กลุ่มต้องออกแบบเอง
     */
    public final int sequence;

    public Job(String id, long arrivalMs, int priority, long workMs, ResourceType resource, long resourceMs, int sequence)
    {
        this.id = id;
        this.arrivalMs = arrivalMs;
        this.priority = priority;
        this.workMs = workMs;
        this.resource = resource;
        this.resourceMs = resourceMs;
        this.sequence = sequence;
    }

    // =====================================================================
    // TODO (นักศึกษา): เพิ่มฟิลด์สำหรับเก็บค่าที่ใช้วัดผลของงานชิ้นนี้เอง
    //
    // ค่าที่โครงงานต้องการ (ดูหัวข้อ 8 ของเอกสารโจทย์):
    //   - เวลาที่เข้าสู่ระบบจริง
    //   - เวลาที่เริ่มถูกทำโดย Worker
    //   - เวลาที่ทำเสร็จ
    //   - เวลาที่เริ่มรอ resource และเวลารอ resource รวม
    //
    // สามคำถามที่ต้องตอบให้ได้ก่อนเขียน และจะถูกถามใน Demo:
    //   1. ใช้เวลาจากนาฬิกาตัวไหน (ดู ProjectLogger.now() ซึ่งให้เวลาฐานเดียว
    //      กับที่ปรากฏใน log ทำให้ค่าที่วัดกับ log ตรวจสอบกันได้)
    //   2. ฟิลด์ใดถูกเขียนโดย Thread หนึ่งแล้วอ่านโดยอีก Thread หนึ่ง
    //      และต้องป้องกันอย่างไร
    //   3. ผลที่ได้ต้องสอดคล้องกับสมการตรวจสอบในหัวข้อ 8:
    //      Turnaround = Waiting + workMs + Resource Wait + resourceMs
    // =========================== Answer =================================
    //
    //   1. ใช้เวลาจาก ProjectLogger.now() ซึ่งใช้ nanoTime ที่เป็น monotonic clock ที่เดินไปข้างหน้าเสมอ ทำให้ค่าที่วัดกับ log ตรวจสอบกันได้
    //
    //   2. actualArrivalMs, startMs, completionMs, resourceWaitStartMs, resourceWaitMs
    //      แก้โดยประกาศเป็น volatile เพื่อให้การอ่านและเขียนเป็นไปอย่างถูกต้องและมองเห็นค่าล่าสุด
    //   ** Turn around time(TAT) = เวลาตั้งแต่เข้างานจนจบงาน
    //


    /** ค่าเริ่มต้นของเวลาที่ยังไม่ถูกบันทึก */
    public static final long UNSET = -1L;

    /** เวลาที่เข้าสู่ระบบจริง (ms) — JobGenerator เป็นคนบันทึก */
    private volatile long actualArrivalMs = UNSET;

    /** เวลาที่ Worker เริ่มทำงานนี้ (ms) */
    private volatile long startMs = UNSET;

    /** เวลาที่งานเสร็จสมบูรณ์ (ms) */
    private volatile long completionMs = UNSET;

    /** เวลาที่เริ่มรอ resource ครั้งล่าสุด (ms) ใช้คู่กับ markResourceAcquired */
    private volatile long resourceWaitStartMs = UNSET;

    /** เวลารอ resource รวม (ms) — เป็น 0 เสมอเมื่อ resource เป็น NONE */
    private volatile long resourceWaitMs = 0L;

    /** JobGenerator เรียกตอนปล่อยงานเข้าระบบ ส่งค่า logger.now() */
    public void markArrived(long nowMs) 
    {
        this.actualArrivalMs = nowMs;
    }

    /** Worker เรียกตอนรับงานไปเริ่มทำ (ขั้นที่ 1) */
    public void markStarted(long nowMs) 
    {
        this.startMs = nowMs;
    }

    /** Worker เรียกก่อน acquire (ขั้นที่ 3) */
    public void markResourceWaitStarted(long nowMs) 
    {
        this.resourceWaitStartMs = nowMs;
    }

    /**
     * Worker เรียกหลัง acquire สำเร็จ (ขั้นที่ 4)
     *
     * @return เวลาที่รอครั้งนี้ (ms) เพื่อส่งต่อให้ logger.resourceAcquired(job, waited)
     */
    public long markResourceAcquired(long nowMs) 
    {
        long waited = nowMs - resourceWaitStartMs;
        this.resourceWaitMs = this.resourceWaitMs + waited;
        return waited;
    }

    /** Worker เรียกตอนงานเสร็จ (ขั้นที่ 7) */
    public void markCompleted(long nowMs) 
    {
        this.completionMs = nowMs;
    }

    public long getActualArrivalMs() { return actualArrivalMs; }
    public long getStartMs() { return startMs; }
    public long getCompletionMs() { return completionMs; }
    public long getResourceWaitMs() { return resourceWaitMs; }

    /** Waiting Time = startTime - actualArrivalTime */
    public long waitingMs() 
    {
        return startMs - actualArrivalMs;
    }

    /** Turnaround Time = completionTime - actualArrivalTime */
    public long turnaroundMs() 
    {
        return completionMs - actualArrivalMs;
    }

    /** true เมื่อบันทึกเวลาครบ 3 จุดหลักแล้ว (ใช้ตรวจก่อนคำนวณสถิติ) */
    public boolean isMeasured() 
    {
        return actualArrivalMs != UNSET && startMs != UNSET && completionMs != UNSET;
    }

    @Override
    public String toString() 
    {
        return String.format("%s(priority=%d, work=%dms, %s)", id, priority, workMs, resource == ResourceType.NONE ? "no resource": resource + " " + resourceMs + "ms");
    }
}
