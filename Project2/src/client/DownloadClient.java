package client;

import common.HashUtil;
import common.Protocol;
import common.Protocol.FileInfo;
import common.Protocol.ProtocolException;
import common.Range;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * DownloadClient.java
 * ------------------------------------------------------------
 * จุดเริ่มต้นของฝั่ง Client: ดาวน์โหลดไฟล์หนึ่งไฟล์แบบแบ่งเป็นหลายช่วง โหลดพร้อมกันหลาย worker
 *
 * ลำดับการทำงาน
 *   1) INFO <filename>            ถาม server ว่าไฟล์ใหญ่กี่ byte
 *   2) RangePlanner.plan()        แบ่งไฟล์เป็น N ช่วงที่ต่อกันพอดี ไม่ซ้อน (ช่วงสุดท้ายรับเศษ)
 *   3) สร้าง N DownloadWorker     รันพร้อมกันใน ExecutorService (ทุกตัวต่อ socket ของตัวเอง)
 *   4) รอครบทุกตัว               ตัวไหนล้มเหลวจะหยุดทั้งหมดทันที
 *   5) merge (เฉพาะแบบ parts)    ต่อ part file ตามลำดับ index เป็นไฟล์เดียว
 *   6) ตรวจ size + คำนวณ SHA-256  แล้วพิมพ์เวลา / throughput
 *
 * วิธีเขียนไฟล์ (เลือกด้วย --write)
 *   parts  worker เขียน part file ของตัวเอง (test.bin.part0 ...) แล้วต่อกันตอนท้าย  (มีขั้น merge เพิ่ม)
 *   direct worker เขียนลงไฟล์จริงตรงตำแหน่ง offset ของตัวเอง ไม่ต้อง merge        (ไฟล์ถูกจองขนาดไว้ก่อน)
 *   ทั้งสองแบบ worker ใช้โค้ดเดียวกัน แค่ได้ "ไฟล์ปลายทาง + ตำแหน่งเริ่มเขียน" ต่างกัน
 *
 * วิธีรัน
 *   java -cp out client.DownloadClient <host> <port>                     แสดงรายการไฟล์ (LIST)
 *   java -cp out client.DownloadClient <host> <port> <filename> [options]
 *
 *   --mode traditional|nio    วิธีอ่าน/เขียนของ worker      (ค่าเริ่มต้น traditional)
 *   --workers N               จำนวน worker                  (ค่าเริ่มต้น 10)
 *   --out DIR                 โฟลเดอร์เก็บไฟล์ที่โหลดมา       (ค่าเริ่มต้น downloads)
 *   --write parts|direct      วิธีเขียนไฟล์                  (ค่าเริ่มต้น parts)
 *   --verify FILE             ไฟล์ต้นฉบับไว้เทียบ SHA-256   (ถ้ารันบนเครื่องเดียวกับ server)
 *
 * ตัวอย่าง
 *   java -cp out client.DownloadClient localhost 9000 test.bin --mode nio --workers 10 --verify files/test.bin
 *
 * ใช้เป็นไลบรารีได้ด้วย: BenchmarkRunner เรียก download(Config) ตรง ๆ แล้วได้ Result กลับไปบันทึก
 *
 * สัญญาที่ไฟล์ถัดไปต้องมีให้ตรง
 *   RangePlanner:
 *     static List<Range> plan(long fileSize, int workers)
 *       คืนช่วงเรียงตาม index เริ่มที่ offset 0 แต่ละช่วงต่อกันพอดี และช่วงสุดท้ายจบที่ fileSize
 *   DownloadWorker (implements Callable<Long> คืนจำนวน byte ที่รับได้):
 *     enum DownloadWorker.Mode { TRADITIONAL, NIO }
 *     new DownloadWorker(String host, int port, String filename, Range range,
 *                        Path target, long targetOffset, DownloadWorker.Mode mode)
 *       ส่ง GET ขอ range ของตัวเอง แล้วเขียน byte ที่ได้ลง target เริ่มที่ targetOffset
 *       (parts: target = part file, targetOffset = 0 / direct: target = ไฟล์จริง, targetOffset = range.offset())
 */
public final class DownloadClient 
{

    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 30_000;
    private static final double MIB = 1024.0 * 1024.0;

    private DownloadClient() {} // ใช้เป็นคลาสเครื่องมือ + main อย่างเดียว

    // ============================================================
    // ชนิดข้อมูล
    // ============================================================

    /** วิธีเขียนไฟล์ปลายทาง */
    public enum WriteStrategy 
    {
        PARTS,
        DIRECT
    }

    /** ค่าตั้งของการดาวน์โหลดหนึ่งครั้ง (verifyFile เป็น null ได้ = ไม่เทียบ hash) */
    public record Config(String host, int port, String filename, DownloadWorker.Mode mode,
                         int workers, Path outDir, WriteStrategy write, Path verifyFile) 
    {
        public Config 
        {
            if (host == null || host.isBlank()) 
            {
                throw new IllegalArgumentException("host must not be empty");
            }
            if (port < 1 || port > 65535) 
            {
                throw new IllegalArgumentException("port must be 1-65535: " + port);
            }
            if (filename == null || filename.isBlank()) 
            {
                throw new IllegalArgumentException("filename must not be empty");
            }
            if (workers < 1) 
            {
                throw new IllegalArgumentException("workers must be at least 1: " + workers);
            }
        }
    }

    /**
     * ผลของการดาวน์โหลดหนึ่งครั้ง
     *   downloadSeconds = ช่วงที่ worker ทำงาน (รวมการจองไฟล์ในแบบ direct)
     *   mergeSeconds    = ช่วงต่อ part file (แบบ direct เป็น 0)
     *   totalSeconds    = download + merge  ใช้คำนวณ throughput (ไม่รวมเวลาคำนวณ hash)
     */
    public record Result(long fileSize, int workers, double downloadSeconds, double mergeSeconds,
                         double totalSeconds, double mbPerSec, String sha256, String expectedSha256) 
    {
        /** true ถ้าไม่ได้ขอเทียบ hash หรือเทียบแล้วตรงกัน */
        public boolean ok() 
        {
            return expectedSha256 == null || HashUtil.matches(expectedSha256, sha256);
        }
    }

    // ============================================================
    // main
    // ============================================================

    public static void main(String[] args) 
    {
        if (args.length < 2) 
        {
            printUsage();
            System.exit(1);
        }
        try 
        {
            String host = args[0];
            int port = Integer.parseInt(args[1]);

            if (args.length == 2) 
            {
                printList(host, port);
                return;
            }

            Config cfg = parseConfig(host, port, args);
            Result r = download(cfg);
            printResult(cfg, r);
            if (!r.ok()) 
            {
                System.exit(2); // hash ไม่ตรงกับต้นฉบับ
            }
        } 
        catch (IllegalArgumentException e) 
        {
            System.err.println("Bad argument: " + e.getMessage());
            printUsage();
            System.exit(1);
        } 
        catch (ProtocolException e) 
        {
            System.err.println("Server replied ERROR " + e.getCode() + ": " + e.getMessage());
            System.exit(1);
        } 
        catch (IOException e) 
        {
            System.err.println("Download failed: " + e.getMessage());
            System.exit(1);
        } 
        catch (InterruptedException e) 
        {
            Thread.currentThread().interrupt();
            System.err.println("Interrupted");
            System.exit(1);
        }
    }

    private static void printUsage() 
    {
        System.err.println("usage: java -cp out client.DownloadClient <host> <port> [<filename> "
                + "[--mode traditional|nio] [--workers N] [--out DIR] [--write parts|direct] [--verify FILE]]");
    }

    private static Config parseConfig(String host, int port, String[] args) 
    {
        String filename = args[2];
        DownloadWorker.Mode mode = DownloadWorker.Mode.TRADITIONAL;
        int workers = 10;
        Path outDir = Path.of("downloads");
        WriteStrategy write = WriteStrategy.PARTS;
        Path verify = null;

        for (int i = 3; i < args.length; i++) 
        {
            String opt = args[i];
            if (i + 1 >= args.length) 
            {
                throw new IllegalArgumentException("missing value for " + opt);
            }
            String value = args[++i];
            switch (opt) 
            {
                case "--mode":
                    mode = parseEnum(DownloadWorker.Mode.class, value, "mode");
                    break;
                case "--workers":
                    workers = Integer.parseInt(value);
                    break;
                case "--out":
                    outDir = Path.of(value);
                    break;
                case "--write":
                    write = parseEnum(WriteStrategy.class, value, "write");
                    break;
                case "--verify":
                    verify = Path.of(value);
                    break;
                default:
                    throw new IllegalArgumentException("unknown option: " + opt);
            }
        }
        return new Config(host, port, filename, mode, workers, outDir, write, verify);
    }

    private static <E extends Enum<E>> E parseEnum(Class<E> type, String text, String name) 
    {
        try 
        {
            return Enum.valueOf(type, text.toUpperCase(Locale.ROOT));
        } 
        catch (IllegalArgumentException e) 
        {
            throw new IllegalArgumentException("invalid " + name + ": " + text);
        }
    }

    // ============================================================
    // ขั้นตอนหลัก
    // ============================================================

    /** ดาวน์โหลดตาม Config แล้วคืนผลลัพธ์ (โยน exception ถ้าล้มเหลวหรือขนาดไฟล์ไม่ตรง) */
    public static Result download(Config cfg) throws IOException, ProtocolException, InterruptedException 
    {
        // 1) ถามขนาดไฟล์
        long size = fetchFileSize(cfg.host(), cfg.port(), cfg.filename());

        // 2) แบ่งช่วง แล้วตรวจว่า planner ทำถูกก่อนเริ่มโหลดจริง
        List<Range> ranges = new ArrayList<>(RangePlanner.plan(size, cfg.workers()));
        ranges.sort(Comparator.comparingInt(Range::index));
        checkPlan(ranges, size);

        Files.createDirectories(cfg.outDir());
        Path finalFile = cfg.outDir().resolve(cfg.filename());

        // 3) เริ่มจับเวลา ตั้งแต่จองไฟล์ (แบบ direct) จนถึงต่อไฟล์เสร็จ (แบบ parts)
        long t0 = System.nanoTime();

        if (cfg.write() == WriteStrategy.DIRECT) 
        {
            // จองขนาดไฟล์ไว้ก่อน เพื่อให้ทุก worker เขียนตามตำแหน่งของตัวเองได้
            try (RandomAccessFile raf = new RandomAccessFile(finalFile.toFile(), "rw")) 
            {
                raf.setLength(size);
            }
        }

        // 4) รัน worker พร้อมกัน แล้วรอครบ
        long received = runWorkers(cfg, ranges, finalFile);
        if (received != size) 
        {
            throw new IOException("received " + received + " bytes, expected " + size);
        }
        long t1 = System.nanoTime();

        // 5) ต่อ part file (เฉพาะแบบ parts)
        if (cfg.write() == WriteStrategy.PARTS) 
        {
            mergeParts(ranges, cfg.filename(), cfg.outDir(), finalFile);
        }
        long t2 = System.nanoTime();

        // 6) ตรวจขนาด แล้วคำนวณ hash (ไม่นับรวมในเวลา)
        long actualSize = Files.size(finalFile);
        if (actualSize != size) 
        {
            throw new IOException("size mismatch: file is " + actualSize + " bytes, expected " + size);
        }
        String sha = HashUtil.sha256Hex(finalFile);
        String expected = cfg.verifyFile() == null ? null : HashUtil.sha256Hex(cfg.verifyFile());

        double download = (t1 - t0) / 1e9;
        double merge = (t2 - t1) / 1e9;
        double total = (t2 - t0) / 1e9;
        double mbps = total > 0 ? (size / MIB) / total : 0;
        return new Result(size, ranges.size(), download, merge, total, mbps, sha, expected);
    }

    /** ส่ง INFO ไปถามขนาดไฟล์ ด้วย connection สั้น ๆ ที่เปิดแล้วปิดทันที */
    public static long fetchFileSize(String host, int port, String filename) throws IOException, ProtocolException 
    {
        try (Socket s = openSocket(host, port)) 
        {
            OutputStream out = s.getOutputStream();
            InputStream in = s.getInputStream(); // ไม่ห่อ buffer ตามที่ Protocol.readLine ต้องการ
            out.write(Protocol.infoRequest(filename).getBytes(StandardCharsets.UTF_8));
            out.flush();
            return Protocol.parseSizeResponse(Protocol.readLine(in));
        }
    }

    /** ส่ง LIST แล้วอ่านรายการไฟล์จนเจอ END */
    public static List<FileInfo> listFiles(String host, int port) throws IOException, ProtocolException 
    {
        try (Socket s = openSocket(host, port)) 
        {
            OutputStream out = s.getOutputStream();
            InputStream in = s.getInputStream();
            out.write(Protocol.listRequest().getBytes(StandardCharsets.UTF_8));
            out.flush();

            List<FileInfo> files = new ArrayList<>();
            while (true) 
            {
                String line = Protocol.readLine(in);
                if (line == null) 
                {
                    throw new ProtocolException(Protocol.ERR_INTERNAL, "connection closed before END");
                }
                if (line.equals(Protocol.RES_END)) 
                {
                    return files;
                }
                files.add(Protocol.parseFileLine(line));
            }
        }
    }

    private static Socket openSocket(String host, int port) throws IOException 
    {
        Socket s = new Socket();
        try 
        {
            s.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
            s.setSoTimeout(READ_TIMEOUT_MS);
            return s;
        } 
        catch (IOException e) 
        {
            s.close();
            throw e;
        }
    }

    // ============================================================
    // worker
    // ============================================================

    /**
     * สร้าง worker ตามจำนวนช่วง รันพร้อมกัน แล้วรอจนครบ
     * ใช้ CompletionService เพื่อรู้ทันทีที่ตัวไหนล้มเหลว (ไม่ต้องรอ worker ตัวก่อนหน้าจบ)
     * ถ้ามีตัวล้มเหลวจะ shutdownNow() หยุดตัวที่เหลือ แล้วรายงาน error ของตัวนั้น
     *
     * @return จำนวน byte ที่ทุก worker รับมารวมกัน
     */
    private static long runWorkers(Config cfg, List<Range> ranges, Path finalFile) 
            throws IOException, InterruptedException 
    {
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, ranges.size()));
        try 
        {
            ExecutorCompletionService<Long> done = new ExecutorCompletionService<>(pool);
            for (Range r : ranges) 
            {
                Path target;
                long targetOffset;
                if (cfg.write() == WriteStrategy.PARTS) 
                {
                    target = cfg.outDir().resolve(r.partFileName(cfg.filename()));
                    targetOffset = 0;
                } 
                else 
                {
                    target = finalFile;
                    targetOffset = r.offset();
                }
                done.submit(new DownloadWorker(cfg.host(), cfg.port(), cfg.filename(), r,
                        target, targetOffset, cfg.mode()));
            }

            long total = 0;
            for (int i = 0; i < ranges.size(); i++) 
            {
                try 
                {
                    total += done.take().get();
                } 
                catch (ExecutionException e) 
                {
                    Throwable cause = e.getCause();
                    throw new IOException("worker failed: " + cause, cause);
                }
            }
            return total;
        } 
        finally 
        {
            pool.shutdownNow(); // ถ้าสำเร็จครบแล้วไม่มีอะไรค้าง ถ้าล้มเหลวจะหยุดตัวที่เหลือ
        }
    }

    // ============================================================
    // ตรวจแผน / merge
    // ============================================================

    /** ตรวจว่าช่วงเริ่มที่ 0 ต่อกันพอดีไม่ซ้อนไม่ขาด และจบที่ขนาดไฟล์ (จับบั๊กใน RangePlanner ได้เร็ว) */
    private static void checkPlan(List<Range> ranges, long size) 
    {
        long expectedOffset = 0;
        for (Range r : ranges) 
        {
            if (r.offset() != expectedOffset) 
            {
                throw new IllegalStateException("bad plan: " + r + " should start at " + expectedOffset);
            }
            expectedOffset = r.end();
        }
        if (expectedOffset != size) 
        {
            throw new IllegalStateException("bad plan: ranges end at " + expectedOffset + " but file size is " + size);
        }
    }

    /** ต่อ part file ตามลำดับ index เป็นไฟล์เดียว แล้วลบ part file ทิ้ง */
    private static void mergeParts(List<Range> ranges, String filename, Path outDir, Path finalFile) 
            throws IOException 
    {
        try (FileChannel dst = FileChannel.open(finalFile,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) 
        {
            for (Range r : ranges) 
            {
                Path part = outDir.resolve(r.partFileName(filename));
                try (FileChannel src = FileChannel.open(part, StandardOpenOption.READ)) 
                {
                    long position = 0;
                    long partSize = src.size();
                    while (position < partSize) 
                    {
                        position += src.transferTo(position, partSize - position, dst);
                    }
                }
                Files.delete(part);
            }
        }
    }

    // ============================================================
    // แสดงผล
    // ============================================================

    private static void printList(String host, int port) throws IOException, ProtocolException 
    {
        List<FileInfo> files = listFiles(host, port);
        if (files.isEmpty()) 
        {
            System.out.println("(no files)");
            return;
        }
        for (FileInfo f : files) 
        {
            System.out.printf(Locale.ROOT, "%-30s %,15d bytes%n", f.name(), f.size());
        }
    }

    private static void printResult(Config cfg, Result r) 
    {
        System.out.printf(Locale.ROOT, "file       : %s (%,d bytes)%n", cfg.filename(), r.fileSize());
        System.out.printf(Locale.ROOT, "setup      : mode=%s workers=%d write=%s%n",
                cfg.mode(), r.workers(), cfg.write());
        System.out.printf(Locale.ROOT, "download   : %.3f s%n", r.downloadSeconds());
        System.out.printf(Locale.ROOT, "merge      : %.3f s%n", r.mergeSeconds());
        System.out.printf(Locale.ROOT, "total      : %.3f s%n", r.totalSeconds());
        System.out.printf(Locale.ROOT, "throughput : %.2f MB/s%n", r.mbPerSec());
        System.out.println("sha256     : " + r.sha256());
        if (r.expectedSha256() == null) 
        {
            System.out.println("verify     : skipped (no --verify file)");
        } 
        else 
        {
            System.out.println("verify     : " + (r.ok() ? "OK (hash matches)" : "FAILED (hash differs)"));
        }
    }
}
