package common;

/**
 * Range.java
 * ------------------------------------------------------------
 * ข้อมูลของ "ช่วงไฟล์ 1 ช่วง" ที่ worker 1 ตัวต้องดาวน์โหลด
 *
 *   index  = ลำดับของ worker/ช่วง (0 ถึง 9 เมื่อใช้ 10 workers)
 *   offset = ตำแหน่ง byte เริ่มต้นในไฟล์ (นับจาก 0)
 *   length = จำนวน byte ที่ต้องรับ
 *
 * ตัวอย่าง: ไฟล์ 1000 byte แบ่ง 10 ช่วง
 *   Range(0, 0, 100), Range(1, 100, 100), ... , Range(9, 900, 100)
 *
 * เป็น record คือคลาสที่เก็บข้อมูลอย่างเดียว แก้ค่าไม่ได้หลังสร้าง
 * ปลอดภัยเมื่อส่งต่อระหว่าง thread (RangePlanner สร้าง -> DownloadWorker ใช้)
 */
public record Range(int index, long offset, long length) 
{

    /** ตรวจค่าตอนสร้าง ถ้าผิดจะ error ทันที ช่วยจับบั๊กใน RangePlanner ได้เร็ว */
    public Range 
    {
        if (index < 0) 
        {
            throw new IllegalArgumentException("index must not be negative: " + index);
        }
        if (offset < 0) 
        {
            throw new IllegalArgumentException("offset must not be negative: " + offset);
        }
        if (length < 0) 
        {
            throw new IllegalArgumentException("length must not be negative: " + length);
        }
    }

    /** ตำแหน่ง byte ถัดจากช่วงนี้ (ไม่รวมตัวมันเอง) ใช้ตรวจว่าช่วงต่อกันพอดีไม่ซ้อน */
    public long end()
    {
        return offset + length;
    }

    /** ชื่อ part file ของช่วงนี้ เช่น "test.bin.part3" */
    public String partFileName(String baseName) 
    {
        return baseName + ".part" + index;
    }

    @Override
    public String toString() 
    {
        return "Range#" + index + "[" + offset + ", " + end() + ") len=" + length;
    }
}