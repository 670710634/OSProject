package bench;

import client.DownloadClient;
import client.DownloadClient.Config;
import client.DownloadClient.Result;
import client.DownloadClient.WriteStrategy;
import client.DownloadWorker;
import common.HashUtil;
import server.FileServer;
import server.FileStore;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.ToDoubleFunction;

/**
 * BenchmarkRunner.java
 * ------------------------------------------------------------
 * รันการทดลองเปรียบเทียบ Traditional I/O กับ NIO ให้อัตโนมัติ แล้วบันทึกผลลง CSV
 *
 * กรณีทดสอบ = (โหมด I/O) x (จำนวน worker)   ค่าเริ่มต้น: {traditional, nio} x {1, 10} = 4 กรณี
 * แต่ละกรณีรัน --runs รอบ (ค่าเริ่มต้น 3)  รวมอย่างน้อย 4 x 3 = 12 รอบ ตรงตามโจทย์
 *
 * วิธีรัน (ไม่ต้องเปิด server เอง โปรแกรมนี้เปิดให้ในตัว)
 *   java -cp out bench.BenchmarkRunner [options]
 *
 *   --dir DIR        โฟลเดอร์ไฟล์ของ server                       (ค่าเริ่มต้น files)
 *   --file NAME      ชื่อไฟล์ที่ใช้โหลด                            (ค่าเริ่มต้น test.bin)
 *   --size-mb N      ถ้ายังไม่มีไฟล์ จะสร้างไฟล์สุ่มขนาด N MB ให้    (ค่าเริ่มต้น 512)
 *   --runs N         จำนวนรอบต่อกรณี                              (ค่าเริ่มต้น 3)
 *   --warmup N       จำนวนรอบอุ่นเครื่องต่อกรณี (ไม่บันทึกผล)        (ค่าเริ่มต้น 1)
 *   --workers LIST   จำนวน worker ที่ทดสอบ คั่นด้วย , เช่น 1,10     (ค่าเริ่มต้น 1,10)
 *   --write MODE     parts | direct                               (ค่าเริ่มต้น parts)
 *   --out FILE       ไฟล์ผลลัพธ์ CSV                              (ค่าเริ่มต้น results.csv)
 *   --port N         พอร์ตของ server แบบ traditional (nio ใช้ N+1)  (ค่าเริ่มต้น 9000)
 *   --threads KIND   pool | virtual  แบบ thread ของ server        (ค่าเริ่มต้น pool)
 *
 * ผลลัพธ์
 *   results.csv          ผลดิบทุกรอบ (1 บรรทัดต่อ 1 รอบ) ใช้ทำตาราง/กราฟในรายงาน
 *   results_summary.csv  สรุปต่อกรณี (ค่าเฉลี่ย, ส่วนเบี่ยงเบนมาตรฐาน, min, max)
 *   และพิมพ์ตารางสรุป + อัตราส่วนเปรียบเทียบบนหน้าจอ
 *
 * ออกแบบเพื่อให้การวัดยุติธรรม
 *   1) เปิด server ทั้งสองโหมดไว้พร้อมกัน (traditional ที่ port, nio ที่ port+1)
 *      จึงสลับกรณีทดสอบได้ทันทีโดยไม่ต้องปิด/เปิด server
 *   2) โหมดของ client ตรงกับโหมดของ server เสมอ (traditional <-> traditional, nio <-> nio)
 *   3) server ใช้ thread แบบเดียวกันทุกกรณี (ค่าเริ่มต้น pool) ความต่างของ thread จะได้ไม่ปนเข้ามา
 *   4) มีรอบอุ่นเครื่อง (JIT + page cache) ที่ไม่บันทึกผล
 *   5) สลับลำดับกรณีทดสอบทุกรอบ ไม่ให้กรณีใดได้เปรียบเสียเปรียบจากลำดับ
 *   6) ลบไฟล์ที่ดาวน์โหลดก่อน/หลังทุกรอบ เริ่มจากสภาพเดียวกันทุกครั้ง
 *   7) เวลาที่วัดคือช่วงโหลด + merge เท่านั้น ไม่รวมเวลาคำนวณ SHA-256 (ดูใน DownloadClient)
 *
 * ข้อจำกัด (ควรเขียนในรายงาน)
 *   - server กับ client อยู่ใน JVM เดียวกัน เครื่องเดียวกัน ผ่าน loopback (localhost)
 *     ไม่มีเครือข่ายจริงมาเป็นคอขวด ไฟล์อยู่ใน page cache ของ OS หลังรอบแรก
 *     ผลจึงต่างจากการใช้งานข้ามเครื่องจริง
 *   - MB/s ในผลลัพธ์ = MiB/s (1 MiB = 1,048,576 byte) ตามที่ DownloadClient คำนวณ
 *   - ไม่ได้วัดการใช้ CPU แยกต่างหาก (ต้องรัน server/client เป็นคนละโปรเซส)
 */
public final class BenchmarkRunner
{

    private static final String HOST = "127.0.0.1";
    private static final int SERVER_POOL_SIZE = 32;
    private static final int STARTUP_WAIT_MS = 5_000;
    private static final Path DOWNLOAD_DIR = Path.of("downloads", "bench");

    private BenchmarkRunner() {} // ใช้เป็นคลาสเครื่องมือ + main อย่างเดียว

    // ============================================================
    // ชนิดข้อมูล
    // ============================================================

    private record Settings(Path dir, String file, long sizeMb, int runs, int warmup,
                            List<Integer> workers, WriteStrategy write, Path csv,
                            int port, String threads) {}

    /** กรณีทดสอบหนึ่งกรณี */
    private record Case(DownloadWorker.Mode mode, int workers)
    {
        String label()
        {
            return mode.name().toLowerCase(Locale.ROOT) + " w=" + workers;
        }
    }

    /** ผลหนึ่งรอบ */
    private record Sample(double downloadS, double mergeS, double totalS, double mbPerSec,
                            String sha256, boolean hashOk) {}

    // ============================================================
    // main
    // ============================================================

    public static void main(String[] args)
    {
        try
        {
            Settings settings = parseSettings(args);
            System.exit(run(settings));
        }
        catch (IllegalArgumentException e)
        {
            System.err.println("Bad argument: " + e.getMessage());
            printUsage();
            System.exit(1);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            System.err.println("Interrupted");
            System.exit(1);
        }
        catch (Exception e)
        {
            System.err.println("Benchmark failed: " + e);
            System.exit(1);
        }
    }

    private static void printUsage()
    {
        System.err.println("usage: java -cp out bench.BenchmarkRunner [--dir DIR] [--file NAME] [--size-mb N] "
                + "[--runs N] [--warmup N] [--workers 1,10] [--write parts|direct] [--out FILE] "
                + "[--port N] [--threads pool|virtual]");
    }

    private static Settings parseSettings(String[] args)
    {
        Path dir = Path.of("files");
        String file = "test.bin";
        long sizeMb = 512;
        int runs = 3;
        int warmup = 1;
        List<Integer> workers = List.of(1, 10);
        WriteStrategy write = WriteStrategy.PARTS;
        Path csv = Path.of("results.csv");
        int port = 9000;
        String threads = "pool";

        for (int i = 0; i < args.length; i++)
        {
            String opt = args[i];
            if (i + 1 >= args.length)
            {
                throw new IllegalArgumentException("missing value for " + opt);
            }
            String value = args[++i];
            switch (opt)
            {
                case "--dir":
                    dir = Path.of(value);
                    break;
                case "--file":
                    file = value;
                    break;
                case "--size-mb":
                    sizeMb = Long.parseLong(value);
                    break;
                case "--runs":
                    runs = Integer.parseInt(value);
                    break;
                case "--warmup":
                    warmup = Integer.parseInt(value);
                    break;
                case "--workers":
                    workers = parseWorkers(value);
                    break;
                case "--write":
                    write = parseWrite(value);
                    break;
                case "--out":
                    csv = Path.of(value);
                    break;
                case "--port":
                    port = Integer.parseInt(value);
                    break;
                case "--threads":
                    threads = value.toLowerCase(Locale.ROOT);
                    break;
                default:
                    throw new IllegalArgumentException("unknown option: " + opt);
            }
        }

        if (!file.matches("[A-Za-z0-9._-]+"))
        {
            throw new IllegalArgumentException("file name may only contain letters, digits, '.', '_' and '-': " + file);
        }
        if (runs < 1)
        {
            throw new IllegalArgumentException("runs must be at least 1: " + runs);
        }
        if (warmup < 0)
        {
            throw new IllegalArgumentException("warmup must not be negative: " + warmup);
        }
        if (port < 1 || port > 65534)
        {
            throw new IllegalArgumentException("port must be 1-65534 (nio server uses port+1): " + port);
        }
        if (!threads.equals("pool") && !threads.equals("virtual"))
        {
            throw new IllegalArgumentException("threads must be 'pool' or 'virtual': " + threads);
        }
        return new Settings(dir, file, sizeMb, runs, warmup, workers, write, csv, port, threads);
    }

    private static List<Integer> parseWorkers(String text)
    {
        LinkedHashSet<Integer> set = new LinkedHashSet<>(); // ตัดค่าซ้ำ แต่คงลำดับ
        for (String part : text.split(","))
        {
            int n = Integer.parseInt(part.trim());
            if (n < 1)
            {
                throw new IllegalArgumentException("workers must be at least 1: " + n);
            }
            set.add(n);
        }
        return new ArrayList<>(set);
    }

    private static WriteStrategy parseWrite(String text)
    {
        try
        {
            return WriteStrategy.valueOf(text.toUpperCase(Locale.ROOT));
        }
        catch (IllegalArgumentException e)
        {
            throw new IllegalArgumentException("write must be 'parts' or 'direct': " + text);
        }
    }

    // ============================================================
    // ขั้นตอนหลัก
    // ============================================================

    /** @return exit code: 0 = สำเร็จ, 2 = มีรอบที่ hash ไม่ตรงต้นฉบับ */
    private static int run(Settings s) throws Exception
    {
        Files.createDirectories(s.dir());
        Path testFile = s.dir().resolve(s.file());
        ensureTestFile(testFile, s.sizeMb());

        long size = Files.size(testFile);
        String expected = HashUtil.sha256Hex(testFile); // คำนวณครั้งเดียว ใช้เทียบทุกรอบ

        printEnvironment(s, size, expected);

        // กรณีทดสอบ: โหมด x จำนวน worker
        List<Case> cases = new ArrayList<>();
        for (DownloadWorker.Mode mode : DownloadWorker.Mode.values())
        {
            for (int w : s.workers())
            {
                cases.add(new Case(mode, w));
            }
        }

        FileStore store = new FileStore(s.dir());
        List<FileServer> servers = new ArrayList<>();
        Map<Case, List<Sample>> samples = new LinkedHashMap<>();
        boolean allHashOk = true;

        try (BufferedWriter csv = Files.newBufferedWriter(s.csv(), StandardCharsets.UTF_8))
        {
            // เปิด server ทั้งสองโหมดไว้พร้อมกัน
            servers.add(startServer(portOf(s, DownloadWorker.Mode.TRADITIONAL), store,
                    FileServer.Mode.TRADITIONAL, s.threads()));
            servers.add(startServer(portOf(s, DownloadWorker.Mode.NIO), store,
                    FileServer.Mode.NIO, s.threads()));

            csv.write("timestamp,round,io_mode,workers,write,file,size_bytes,"
                    + "download_s,merge_s,total_s,mb_per_s,sha256,hash_ok\n");

            // ----- อุ่นเครื่อง (ไม่บันทึกผล) -----
            for (int w = 1; w <= s.warmup(); w++)
            {
                for (Case c : cases)
                {
                    Sample smp = runOnce(s, c, expected);
                    System.out.printf(Locale.ROOT, "[warmup %d/%d] %-18s total %.3f s%n",
                            w, s.warmup(), c.label(), smp.totalS());
                }
            }

            // ----- วัดผลจริง: สลับลำดับกรณีทดสอบทุกรอบ -----
            for (int round = 1; round <= s.runs(); round++)
            {
                for (int k = 0; k < cases.size(); k++)
                {
                    Case c = cases.get((k + round - 1) % cases.size());
                    Sample smp = runOnce(s, c, expected);

                    samples.computeIfAbsent(c, key -> new ArrayList<>()).add(smp);
                    allHashOk &= smp.hashOk();

                    System.out.printf(Locale.ROOT, "[round %d/%d] %-18s total %.3f s  %.2f MB/s  hash %s%n",
                            round, s.runs(), c.label(), smp.totalS(), smp.mbPerSec(),
                            smp.hashOk() ? "OK" : "MISMATCH");

                    csv.write(String.format(Locale.ROOT, "%s,%d,%s,%d,%s,%s,%d,%.3f,%.3f,%.3f,%.2f,%s,%b%n",
                            Instant.now(), round, c.mode().name().toLowerCase(Locale.ROOT), c.workers(),
                            s.write().name().toLowerCase(Locale.ROOT), s.file(), size,
                            smp.downloadS(), smp.mergeS(), smp.totalS(), smp.mbPerSec(),
                            smp.sha256(), smp.hashOk()));
                    csv.flush(); // บันทึกทีละรอบ ถ้าโปรแกรมหยุดกลางคันผลที่ได้แล้วไม่หาย
                }
            }
        }
        finally
        {
            for (FileServer server : servers)
            {
                server.stop();
            }
            cleanDownloads(s.file());
        }

        printSummary(s, cases, samples);
        System.out.println();
        System.out.println("raw results : " + s.csv().toAbsolutePath());
        if (!allHashOk)
        {
            System.out.println("WARNING: some runs produced a file whose SHA-256 differs from the original");
            return 2;
        }
        return 0;
    }

    /** ดาวน์โหลดหนึ่งรอบ: เริ่มจากโฟลเดอร์ว่าง, ตรวจ hash กับต้นฉบับ, แล้วลบไฟล์ที่ได้ทิ้ง */
    private static Sample runOnce(Settings s, Case c, String expectedSha) throws Exception
    {
        cleanDownloads(s.file());

        // verifyFile = null เพราะเราเทียบ hash เองกับค่าที่คำนวณไว้แล้ว (ไม่ต้องให้ client อ่านต้นฉบับซ้ำทุกรอบ)
        Config cfg = new Config(HOST, portOf(s, c.mode()), s.file(), c.mode(), c.workers(),
                DOWNLOAD_DIR, s.write(), null);
        Result r = DownloadClient.download(cfg);

        cleanDownloads(s.file());
        return new Sample(r.downloadSeconds(), r.mergeSeconds(), r.totalSeconds(), r.mbPerSec(),
                r.sha256(), HashUtil.matches(expectedSha, r.sha256()));
    }

    /** traditional ใช้พอร์ตหลัก, nio ใช้พอร์ตหลัก + 1 */
    private static int portOf(Settings s, DownloadWorker.Mode mode)
    {
        return mode == DownloadWorker.Mode.TRADITIONAL ? s.port() : s.port() + 1;
    }

    // ============================================================
    // server ในตัว
    // ============================================================

    private static FileServer startServer(int port, FileStore store, FileServer.Mode mode, String threads)
            throws IOException, InterruptedException
    {
        // ถ้ามีโปรแกรมอื่นฟังพอร์ตนี้อยู่แล้ว (เช่นเผลอเปิด FileServer ค้างไว้) ต้องหยุด
        // ไม่งั้นเราจะวัดผลกับ server ผิดตัวโดยไม่รู้ตัว
        if (isListening(port))
        {
            throw new IOException("port " + port + " is already in use; stop the other server or use --port");
        }

        ExecutorService executor = threads.equals("virtual")
                ? Executors.newVirtualThreadPerTaskExecutor()
                : Executors.newFixedThreadPool(SERVER_POOL_SIZE);

        FileServer server = new FileServer(port, store, mode, executor);
        Thread thread = new Thread(() ->
        {
            try
            {
                server.start(); // บล็อกจนกว่าจะเรียก stop()
            }
            catch (IOException e)
            {
                System.err.println("server on port " + port + " stopped: " + e.getMessage());
            }
        }, "bench-server-" + mode);
        thread.setDaemon(true);
        thread.start();

        // รอจน server เริ่มรับ connection ได้
        long deadline = System.nanoTime() + STARTUP_WAIT_MS * 1_000_000L;
        while (!isListening(port))
        {
            if (System.nanoTime() > deadline)
            {
                throw new IOException("server did not start listening on port " + port);
            }
            Thread.sleep(50);
        }
        return server;
    }

    private static boolean isListening(int port)
    {
        try (Socket socket = new Socket())
        {
            socket.connect(new InetSocketAddress(HOST, port), 200);
            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }

    // ============================================================
    // ไฟล์ทดสอบ / ไฟล์ที่ดาวน์โหลด
    // ============================================================

    /** ถ้ายังไม่มีไฟล์ทดสอบ สร้างไฟล์ข้อมูลสุ่ม (seed คงที่ เพื่อให้ทำซ้ำได้) */
    private static void ensureTestFile(Path file, long sizeMb) throws IOException
    {
        if (Files.isRegularFile(file))
        {
            return; // มีอยู่แล้ว ใช้ไฟล์เดิมตามขนาดจริง (ไม่สนใจ --size-mb)
        }
        if (sizeMb <= 0)
        {
            throw new IOException("test file not found and --size-mb is not positive: " + file);
        }
        System.out.println("generating " + sizeMb + " MB random test file: " + file.toAbsolutePath());
        Random random = new Random(42);
        byte[] chunk = new byte[1024 * 1024];
        try (OutputStream out = Files.newOutputStream(file))
        {
            for (long i = 0; i < sizeMb; i++)
            {
                random.nextBytes(chunk);
                out.write(chunk);
            }
        }
    }

    /** ลบไฟล์ปลายทางและ part file ที่ค้างอยู่ทั้งหมด (ชื่อไฟล์ผ่านการตรวจแล้วว่าไม่มีอักขระพิเศษของ glob) */
    private static void cleanDownloads(String filename) throws IOException
    {
        Files.createDirectories(DOWNLOAD_DIR);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(DOWNLOAD_DIR, filename + "*"))
        {
            for (Path p : stream)
            {
                Files.deleteIfExists(p);
            }
        }
    }

    // ============================================================
    // แสดงผล / สรุป
    // ============================================================

    private static void printEnvironment(Settings s, long size, String sha)
    {
        System.out.println("=== Benchmark setup ===");
        System.out.printf(Locale.ROOT, "java        : %s (%s)%n", System.getProperty("java.version"), System.getProperty("java.vm.name"));
        System.out.printf(Locale.ROOT, "os / cpus   : %s / %d%n", System.getProperty("os.name"), Runtime.getRuntime().availableProcessors());
        System.out.printf(Locale.ROOT, "file        : %s (%,d bytes, %.1f MiB)%n", s.file(), size, size / (1024.0 * 1024.0));
        System.out.printf(Locale.ROOT, "sha256      : %s%n", sha);
        System.out.printf(Locale.ROOT, "workers     : %s   runs/case: %d   warmup/case: %d%n", s.workers(), s.runs(), s.warmup());
        System.out.printf(Locale.ROOT, "write       : %s   server threads: %s%n", s.write(), s.threads());
        System.out.println("note        : server and client share one JVM and talk over loopback");
        System.out.println();
    }

    private static void printSummary(Settings s, List<Case> cases, Map<Case, List<Sample>> samples)
            throws IOException
    {
        System.out.println();
        System.out.println("=== Summary (" + s.runs() + " runs per case) ===");
        System.out.printf(Locale.ROOT, "%-12s %7s %5s %9s %8s %8s %8s %11s %9s%n",
                "io_mode", "workers", "runs", "mean_s", "sd_s", "min_s", "max_s", "mean_MB/s", "sd_MB/s");

        Path summaryCsv = summaryPath(s.csv());
        try (BufferedWriter out = Files.newBufferedWriter(summaryCsv, StandardCharsets.UTF_8))
        {
            out.write("io_mode,workers,runs,mean_total_s,sd_total_s,min_total_s,max_total_s,mean_mb_per_s,sd_mb_per_s\n");
            for (Case c : cases)
            {
                List<Sample> list = samples.get(c);
                double[] t = column(list, Sample::totalS);
                double[] m = column(list, Sample::mbPerSec);
                String mode = c.mode().name().toLowerCase(Locale.ROOT);

                System.out.printf(Locale.ROOT, "%-12s %7d %5d %9.3f %8.3f %8.3f %8.3f %11.2f %9.2f%n",
                        mode, c.workers(), list.size(), mean(t), sd(t), min(t), max(t), mean(m), sd(m));
                out.write(String.format(Locale.ROOT, "%s,%d,%d,%.3f,%.3f,%.3f,%.3f,%.2f,%.2f%n",
                        mode, c.workers(), list.size(), mean(t), sd(t), min(t), max(t), mean(m), sd(m)));
            }
        }

        // เปรียบเทียบ: ใช้หลาย worker เร็วขึ้นกี่เท่า (ถ้าเร็วขึ้นสมบูรณ์แบบจะเท่ากับจำนวน worker ที่เพิ่ม)
        int minW = Collections.min(s.workers());
        int maxW = Collections.max(s.workers());
        System.out.println();
        if (minW != maxW)
        {
            for (DownloadWorker.Mode mode : DownloadWorker.Mode.values())
            {
                double slow = meanTotal(samples, new Case(mode, minW));
                double fast = meanTotal(samples, new Case(mode, maxW));
                System.out.printf(Locale.ROOT, "speedup %-11s w=%d -> w=%d : %.2fx (ideal %.0fx)%n",
                        mode.name().toLowerCase(Locale.ROOT), minW, maxW, slow / fast, (double) maxW / minW);
            }
        }

        // เปรียบเทียบ: NIO เทียบกับ Traditional ที่จำนวน worker เท่ากัน (> 1 แปลว่า NIO เร็วกว่า)
        for (int w : s.workers())
        {
            double trad = meanTotal(samples, new Case(DownloadWorker.Mode.TRADITIONAL, w));
            double nio = meanTotal(samples, new Case(DownloadWorker.Mode.NIO, w));
            System.out.printf(Locale.ROOT, "traditional/nio time ratio at w=%d : %.2f  (above 1.00 = NIO faster)%n",
                    w, trad / nio);
        }
        System.out.println("summary csv : " + summaryCsv.toAbsolutePath());
    }

    /** results.csv -> results_summary.csv */
    private static Path summaryPath(Path csv)
    {
        String name = csv.getFileName().toString();
        int dot = name.lastIndexOf('.');
        String base = dot > 0 ? name.substring(0, dot) : name;
        return csv.resolveSibling(base + "_summary.csv");
    }

    private static double meanTotal(Map<Case, List<Sample>> samples, Case c)
    {
        return mean(column(samples.get(c), Sample::totalS));
    }

    // ============================================================
    // สถิติพื้นฐาน
    // ============================================================

    private static double[] column(List<Sample> list, ToDoubleFunction<Sample> getter)
    {
        return list.stream().mapToDouble(getter).toArray();
    }

    private static double mean(double[] v)
    {
        return v.length == 0 ? 0 : Arrays.stream(v).sum() / v.length;
    }

    /** ส่วนเบี่ยงเบนมาตรฐานของตัวอย่าง (หารด้วย n-1) ถ้ามีรอบเดียวจะได้ 0 */
    private static double sd(double[] v)
    {
        if (v.length < 2)
        {
            return 0;
        }
        double m = mean(v);
        double sum = 0;
        for (double x : v)
        {
            sum += (x - m) * (x - m);
        }
        return Math.sqrt(sum / (v.length - 1));
    }

    private static double min(double[] v)
    {
        return Arrays.stream(v).min().orElse(0);
    }

    private static double max(double[] v)
    {
        return Arrays.stream(v).max().orElse(0);
    }
}