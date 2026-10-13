# สมาชิกกลุ่ม
## 670710632 รัชชานนท์ เจริญรมย์
## 670710633 รามิล แคใหญ่
## 670710634 วีรศิลป์ ลีนาวงศ์

# วิธี Compile และ Run
## โครงสร้าง Code

```powershell
src
{
    common
    {
        Protocal.java
        Range.java
        HashUtil.java
    }
    server
    {
        ClientHandler.java
        FileServer.java
        FileStore.java
    }
    client
    {
        DownloadClient.java
        DownloadWorker.java
        RangePlanner.java
    }
    bench
    {
        BenchmarkRunner.java
    }
    files
    {
        TargetFile.txt
    }
}
```

## เทียบแบบ Merge & Direct 10 รอบ
### Compile
```bash
javac -d out src\common\*.java src\server\*.java src\client\*.java src\bench\*.java
```
### Run
```bash
# Parts
java -cp out bench.BenchmarkRunner --file TargetFile.txt --runs 10 --workers 1,10 --write parts --out results_parts.csv
# Direct
java -cp out bench.BenchmarkRunner --file TargetFile.txt --runs 10 --workers 1,10 --write direct --out results_direct.csv
```
## ดึง File ที่ต้องการ
### Compile
```powershell
javac -d out (Get-ChildItem -Recurse -Filter *.java src).FullName
```
### Run
```powershell
# Terminal 1 (Server)
java -cp out server.FileServer {1-65534} files {traditional/nio} {pool/virtual}

# Terminal 2 (Client)
java -cp out client.DownloadClient localhost {Port ที่เลือก} #LIST&INFO
java -cp out client.DownloadClient localhost {Port ที่เลือก} TargetFile.txt --mode {traditional/nio} --workers {1-10} --write {parts/direct} --verify files/TargetFile.txt #เริ่มดึงไฟล์

# GET
$c = New-Object System.Net.Sockets.TcpClient("localhost", 9000)
$s = $c.GetStream()
$w = New-Object System.IO.StreamWriter($s); $w.NewLine = "`n"; $w.AutoFlush = $true
$r = New-Object System.IO.StreamReader($s)

$w.WriteLine("GET TargetFile.txt 0 10")
$r.ReadLine()
$buf = New-Object char[] 10
$r.Read($buf, 0, 10) | Out-Null
-join $buf

$c.Close()

## Ctrl + C ที่ Terminal เพื่อหยุดการทำงาน
```
---

# ผลการทดลอง

## วิธี Merge (ค่าเฉลี่ยจาก 10 รอบ)

| โหมด | workers | เวลารวมเฉลี่ย (s) | sd (s) | min (s) | max (s) | throughput เฉลี่ย (MB/s) | sd (MB/s) |
|---|---|---|---|---|---|---|---|
| Traditional | 1 | 1.203 | 0.200 | 0.967 | 1.556 | 435.83 | 69.43 |
| Traditional | 10 | 0.756 | 0.074 | 0.685 | 0.923 | 682.41 | 61.61 |
| NIO | 1 | 1.565 | 0.218 | 1.338 | 1.902 | 332.60 | 43.29 |
| NIO | 10 | 0.847 | 0.060 | 0.764 | 0.942 | 607.44 | 42.48 |

## วิธี Direct (ค่าเฉลี่ยจาก 10 รอบ)

| โหมด | workers | เวลารวมเฉลี่ย (s) | sd (s) | min (s) | max (s) | throughput เฉลี่ย (MB/s) | sd (MB/s) |
|---|---|---|---|---|---|---|---|
| Traditional | 1 | 0.535 | 0.045 | 0.491 | 0.616 | 963.01 | 76.50 |
| Traditional | 10 | 2.220 | 1.766 | 0.605 | 6.177 | 371.75 | 258.95 |
| NIO | 1 | 0.896 | 0.126 | 0.774 | 1.176 | 580.67 | 72.18 |
| NIO | 10 | 4.835 | 2.835 | 0.784 | 9.940 | 176.77 | 179.45 |

## เปรียบเทียบเวลารวมของทั้งสองวิธีเขียน (วินาที)

| โหมด | workers | merge | direct |
|---|---|---|---|
| Traditional | 1 | 1.203 | 0.535 |
| Traditional | 10 | 0.756 | 2.220 |
| NIO | 1 | 1.565 | 0.896 |
| NIO | 10 | 0.847 | 4.835 |

---

# ตอบคำถาม

## ทำไม 10 workers ถึงไม่เร็วขึ้น 10 เท่า

การเพิ่มจำนวน worker ช่วยได้เฉพาะส่วนที่ทุก Worker ทำไปพร้อมๆกันได้ เช่น การดาวน์โหลด ขณะที่มีอีกหลายการทำงานที่เป็นส่วนที่ต้องทำต่อ ๆ กัน

## ทำไม NIO ถึงไม่ชนะทุกครั้ง

NIO ไม่ชนะในการทดลองนี้ เพราะข้อได้เปรียบในเรื่อง zero-copy แสดงผลไม่เต็มที่ เพราะเป็นการส่งแบบ loopback และข้อมูลยังถูก Copy เข้าฝั่งรับอยู่ดี ไม่ใช่ zero-copy จริง

# ข้อจำกัดการทดลอง

1. **Page cache:** หลังรอบแรกไฟล์ต้นทางอยู่ใน RAM แล้ว การอ่านจึงอาจเร็วกว่าของจริงมาก
2. **Loopback network:** ไม่มีเครือข่ายจริง ข้อมูลวิ่งผ่าน memory ภายในเครื่อง และ client กับ server แย่ง CPU เดียวกัน
3. **Storage cache:** การเขียน part file และ merge ลง cache ของ OS ก่อน จึงอาจไม่สะท้อนความเร็วเขียนดิสก์จริง

---
