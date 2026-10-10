package client;

import common.Range;

import java.util.ArrayList;
import java.util.List;

/**
 * RangePlanner.java
 * ------------------------------------------------------------
 * แบ่งไฟล์ขนาด fileSize byte ออกเป็น N ช่วง ให้ worker แต่ละตัวโหลดช่วงของตัวเอง
 *
 * กติกาการแบ่ง (ตรงตามโจทย์)
 *   1) ช่วงแรกเริ่มที่ offset 0
 *   2) ทุกช่วงต่อกันพอดี ไม่ซ้อน ไม่ขาด (end() ของช่วงหนึ่ง = offset ของช่วงถัดไป)
 *   3) ช่วงปกติยาว fileSize / N byte (หารปัดเศษลง)
 *   4) ช่วงสุดท้ายรับส่วนที่เหลือทั้งหมด จึงจบที่ fileSize พอดีเสมอ
 *
 * ตัวอย่าง: ไฟล์ 1003 byte, 10 workers   -> ช่วงละ 100 byte, ช่วงที่ 9 ได้ 103 byte
 *   #0 [0, 100)  #1 [100, 200) ... #8 [800, 900)  #9 [900, 1003)
 *
 * กรณีขอบ
 *   ไฟล์เล็กกว่าจำนวน worker (เช่น 3 byte, 10 workers)
 *       fileSize / N = 0 ช่วงที่ 0-8 จึงยาว 0 byte และช่วงสุดท้ายได้ทั้งไฟล์
 *       ยังถูกต้องตามกติกา (server ตอบ "OK 0" ให้ช่วงที่ยาว 0)
 *   ไฟล์ว่าง (0 byte)  ทุกช่วงยาว 0 byte
 *
 * เป็นฟังก์ชันล้วน ๆ ไม่มี I/O และไม่มี state จึงทดสอบง่ายและเรียกจากหลาย thread ได้
 */
public final class RangePlanner
{

    private RangePlanner() {} // ใช้เป็นคลาสเครื่องมืออย่างเดียว

    /**
     * @param fileSize ขนาดไฟล์เป็น byte (ต้องไม่ติดลบ)
     * @param workers  จำนวนช่วง/worker (ต้องอย่างน้อย 1)
     * @return ช่วงทั้งหมดเรียงตาม index 0..workers-1
     */
    public static List<Range> plan(long fileSize, int workers)
    {
        if (fileSize < 0)
        {
            throw new IllegalArgumentException("fileSize must not be negative: " + fileSize);
        }
        if (workers < 1)
        {
            throw new IllegalArgumentException("workers must be at least 1: " + workers);
        }

        long chunk = fileSize / workers; // ขนาดช่วงปกติ (หารปัดลง)
        List<Range> ranges = new ArrayList<>(workers);

        for (int i = 0; i < workers; i++)
        {
            long offset = (long) i * chunk;
            boolean isLast = (i == workers - 1);
            // ช่วงสุดท้ายยาวถึงท้ายไฟล์ เพื่อเก็บเศษที่หารไม่ลงตัว
            long length = isLast ? fileSize - offset : chunk;
            ranges.add(new Range(i, offset, length));
        }
        return ranges;
    }
}