package server;

import common.Protocol;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * FileServer.java
 * ------------------------------------------------------------
 * จุดเริ่มต้นของฝั่ง Server: เปิด port รอ client แล้วส่งแต่ละ connection
 * ให้ ClientHandler ไปทำงานต่อใน thread ของมันเอง (main thread วนรับ connection อย่างเดียว)
 *
 * วิธีรัน
 *   java -cp out server.FileServer [port] [filesDir] [mode] [threads]
 *
 *   port     พอร์ตที่เปิดรอ              (ค่าเริ่มต้น 9000)
 *   filesDir โฟลเดอร์ไฟล์ที่ให้โหลด       (ค่าเริ่มต้น files)
 *   mode     traditional | nio          (ค่าเริ่มต้น traditional)
 *   threads  virtual | pool             (ค่าเริ่มต้น virtual)
 *
 * ตัวอย่าง
 *   java -cp out server.FileServer 9000 files nio virtual
 *
 * ความต่างของสองโหมด (นี่คือสิ่งที่ benchmark ต้องการเทียบ)
 *   traditional : ServerSocket        -> ClientHandler(Socket, FileStore)
 *                 อ่านไฟล์ด้วย RandomAccessFile แล้วเขียนผ่าน OutputStream
 *   nio         : ServerSocketChannel -> ClientHandler(SocketChannel, FileStore)
 *                 ส่งไฟล์ด้วย FileChannel.transferTo (zero-copy)
 *
 * วิธีจัดการ thread
 *   virtual : หนึ่ง connection ต่อหนึ่ง virtual thread (Java 21+) เบามาก รับ connection ได้เยอะ
 *   pool    : fixed thread pool จำนวนจำกัด connection ที่เกินจะรอคิว
 *
 * สัญญาที่ ClientHandler / FileStore ต้องมี (ไฟล์ถัดไปที่จะเขียน)
 *   new FileStore(Path root) throws IOException
 *   new ClientHandler(Socket socket, FileStore store)           implements Runnable
 *   new ClientHandler(SocketChannel channel, FileStore store)   implements Runnable
 *   ClientHandler ต้องปิด socket/channel เองเมื่อจบงาน
 */
public final class FileServer 
{

    /** โหมดการอ่าน/ส่งไฟล์ */
    public enum Mode 
    {
        TRADITIONAL,
        NIO;

        static Mode parse(String text) 
        {
            switch (text.toLowerCase()) 
            {
                case "traditional":
                    return TRADITIONAL;
                case "nio":
                    return NIO;
                default:
                    throw new IllegalArgumentException("mode must be 'traditional' or 'nio': " + text);
            }
        }
    }

    private static final int POOL_SIZE = 32;          // ขนาด pool เมื่อเลือก threads=pool
    private static final int BACKLOG = 128;           // คิว connection ที่รอ accept
    private static final int SHUTDOWN_WAIT_SEC = 5;   // เวลารอให้งานค้างจบก่อนปิดเซิร์ฟเวอร์

    private final int port;
    private final FileStore store;
    private final Mode mode;
    private final ExecutorService executor;

    private volatile boolean running = true;
    private volatile Closeable listener;              // ServerSocket หรือ ServerSocketChannel ที่เปิดอยู่

    public FileServer(int port, FileStore store, Mode mode, ExecutorService executor) 
    {
        this.port = port;
        this.store = store;
        this.mode = mode;
        this.executor = executor;
    }

    // ============================================================
    // main: อ่าน argument แล้วเริ่มเซิร์ฟเวอร์
    // ============================================================

    public static void main(String[] args) 
    {
        try 
        {
            int port = args.length > 0 ? Integer.parseInt(args[0]) : Protocol.DEFAULT_PORT;
            Path dir = Path.of(args.length > 1 ? args[1] : "files");
            Mode mode = args.length > 2 ? Mode.parse(args[2]) : Mode.TRADITIONAL;
            String threads = args.length > 3 ? args[3].toLowerCase() : "virtual";

            ExecutorService executor = newExecutor(threads);
            FileStore store = new FileStore(dir);

            FileServer server = new FileServer(port, store, mode, executor);
            Runtime.getRuntime().addShutdownHook(new Thread(server::stop, "shutdown-hook"));

            System.out.printf("FileServer: port=%d dir=%s mode=%s threads=%s%n",
                    port, dir.toAbsolutePath(), mode, threads);
            server.start(); // บล็อกอยู่ตรงนี้จนกว่าจะถูกปิด (Ctrl+C)
        } 
        catch (IllegalArgumentException e) 
        {
            System.err.println("Bad argument: " + e.getMessage());
            printUsage();
            System.exit(1);
        } 
        catch (IOException e) 
        {
            System.err.println("Server error: " + e.getMessage());
            System.exit(1);
        }
    }

    private static ExecutorService newExecutor(String threads) 
    {
        switch (threads) 
        {
            case "virtual":
                return Executors.newVirtualThreadPerTaskExecutor();
            case "pool":
                return Executors.newFixedThreadPool(POOL_SIZE);
            default:
                throw new IllegalArgumentException("threads must be 'virtual' or 'pool': " + threads);
        }
    }

    private static void printUsage() 
    {
        System.err.println("usage: java -cp out server.FileServer [port] [filesDir] [traditional|nio] [virtual|pool]");
    }

    // ============================================================
    // วนรับ connection
    // ============================================================

    /** เปิด port แล้ววน accept จนกว่าจะเรียก stop() */
    public void start() throws IOException 
    {
        if (mode == Mode.NIO) 
        {
            acceptLoopNio();
        } 
        else 
        {
            acceptLoopTraditional();
        }
    }

    /** โหมด traditional: ServerSocket.accept() ได้ Socket ส่งให้ handler */
    private void acceptLoopTraditional() throws IOException 
    {
        try (ServerSocket serverSocket = new ServerSocket()) 
        {
            serverSocket.setReuseAddress(true);        // restart เซิร์ฟเวอร์เร็ว ๆ แล้วไม่ติด "Address already in use"
            serverSocket.bind(new InetSocketAddress(port), BACKLOG);
            listener = serverSocket;

            while (running) 
            {
                try 
                {
                    Socket socket = serverSocket.accept();
                    executor.execute(new ClientHandler(socket, store));
                } 
                catch (IOException e) 
                {
                    if (!running) 
                    {
                        break; // ถูกสั่งปิด ไม่ใช่ error จริง
                    }
                    System.err.println("accept failed: " + e.getMessage());
                }
            }
        }
    }

    /** โหมด nio: ServerSocketChannel.accept() ได้ SocketChannel ส่งให้ handler (blocking mode) */
    private void acceptLoopNio() throws IOException 
    {
        try (ServerSocketChannel serverChannel = ServerSocketChannel.open()) 
        {
            serverChannel.setOption(java.net.StandardSocketOptions.SO_REUSEADDR, true);
            serverChannel.bind(new InetSocketAddress(port), BACKLOG);
            listener = serverChannel;

            while (running) 
            {
                try 
                {
                    SocketChannel channel = serverChannel.accept();
                    executor.execute(new ClientHandler(channel, store));
                } 
                catch (IOException e) 
                {
                    if (!running) 
                    {
                        break;
                    }
                    System.err.println("accept failed: " + e.getMessage());
                }
            }
        }
    }

    // ============================================================
    // ปิดเซิร์ฟเวอร์
    // ============================================================

    /** หยุดรับ connection ใหม่ รอให้งานที่ค้างอยู่จบ (ไม่เกิน 5 วินาที) แล้วปิด executor */
    public void stop() 
    {
        running = false;
        try 
        {
            Closeable l = listener;
            if (l != null) 
            {
                l.close(); // ทำให้ accept() ที่บล็อกอยู่โยน exception ออกมาแล้วหลุดลูป
            }
        } 
        catch (IOException ignored) 
        {
            // ปิดอยู่แล้ว ไม่ต้องทำอะไร
        }

        executor.shutdown();
        try 
        {
            if (!executor.awaitTermination(SHUTDOWN_WAIT_SEC, TimeUnit.SECONDS)) 
            {
                executor.shutdownNow();
            }
        } 
        catch (InterruptedException e) 
        {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        System.out.println("FileServer stopped");
    }
}
