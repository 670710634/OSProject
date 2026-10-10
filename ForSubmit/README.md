# วิธีใช้งาน
## 1. Compile
### ใส่ 
```powershell
javac -d out src\*.java
```
### ที่ Terminal เพื่อ Complie ตัวโปรแกม Java ทั้งหมด (แยกไฟล์ที่ compile แล้วไปที่ folder out เพื่อความสะอาดตา)
## 2. Run
### ใส่ 
```powershell
java -cp out Main {ไฟล์ .CSV ที่ต้องการ} {priority / fcfs} {จำนวน Workers} {จำนวน Permits printer} {จำนวน Permits database} > {ชื่อไฟล์ที่ต้องการเก็บ log}
```
### เพื่อรัน Files .CSV ที่ต้องการ เช่น
```powershell
java -cp out Main workloads\jobs_db.csv priority 3 1 2 > logs\db_priority_w3.log
```
## ข้อจำกัดที่ควรรู้
### ค่าการคำนวณอาจคลาดเคลื่อนเล็กน้อยจากการตื่นจาก sleep ช้า ซึ่งไม่ควรเยอะมาก (>50ms)
