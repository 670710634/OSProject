package client;

import common.Protocol;
import common.Protocol.ProtocolException;
import common.Range;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Callable;

/**
 * DownloadWorker.java
 * ------------------------------------------------------------
 * worker 1 ตัว = ดาวน์โหลด "ช่วงไฟล์ 1 ช่วง" ด้วย connection ของตัวเอง
 *
 * ลำดับการทำงานของ call()
 *   1) เปิด connection ไปที่ server (แต่ละ worker มี socket ของตัวเอง ไม่แชร์กัน)
 *   2) ส่ง  GET <filename> <offset> <length>
 *   3) อ่าน header  OK <length>  (ถ้าเป็น ERROR จะโยน ProtocolException พร้อมรหัส)
 *   4) รับ byte ดิบให้ครบ <length> แล้วเขียนลงไฟล์ปลายทางเริ่มที่ targetOffset
 *   5) คืนจำนวน byte ที่รับได้ (DownloadClient เอาไปรวมแล้วเทียบกับขนาดไฟล์)
 *
 * สองโหมด ต่างกันเฉพาะ "ตอนรับ payload แล้วเขียนไฟล์" (ส่วนที่ benchmark ต้องการเทียบ)
 *   TRADITIONAL : Socket + InputStream.read() ลง byte[] แล้ว RandomAccessFile.write()
 *                 ข้อมูลวิ่ง เครือข่าย -> kernel -> heap ของ Java -> kernel -> ดิสก์
 *   NIO         : SocketChannel + FileChannel.transferFrom(channel, position, count)
 *                 ให้ FileChannel ดึงข้อมูลจาก channel แล้วเขียนที่ตำแหน่งที่ระบุ
 *
 * ไฟล์ปลายทางมี 2 แบบ (DownloadClient เป็นคนเลือกแล้วส่งมาให้ ผ่านค่า target / targetOffset)
 *   parts  : target = part file ของ worker นี้ (เช่น test.bin.part3), targetOffset = 0
 *            worker เป็นเจ้าของไฟล์นี้คนเดียว จึงล้างไฟล์เก่า (ถ้ามี) ก่อนเขียน
 *            กันกรณีมี part file ค้างจากรอบก่อนที่ใหญ่กว่า แล้วเศษท้ายไฟล์ติดมาตอน merge
 *   direct : target = ไฟล์จริง (ถูกจองขนาดไว้แล้ว), targetOffset = range.offset()
 *            ไฟล์นี้ใช้ร่วมกับ worker อื่น จึง "ห้ามล้าง/ตัด" ไฟล์ แค่เขียนทับตำแหน่งตัวเอง
 *            ทุก worker เปิด handle ของตัวเอง (position แยกกัน) เขียนคนละช่วงที่ไม่ซ้อนกัน
 *            จึงไม่ต้อง lock
 *
 * ช่วงยาว 0 byte (เกิดเมื่อไฟล์เล็กกว่าจำนวน worker): ไม่ต้องต่อ server เลย
 * แต่แบบ parts ต้องสร้าง part file เปล่าไว้ เพราะขั้น merge จะเปิดอ่านทุก part
 *
 * ข้อควรรู้ (เอาไปอธิบายในรายงานได้)
 *   - FileChannel.transferFrom จาก socket ไม่ใช่ zero-copy จริง ข้างในอ่านจาก channel
 *     ลง buffer ชั่วคราวขนาดเล็ก แล้วเขียนลงไฟล์ ประโยชน์ zero-copy เห็นชัดที่ฝั่ง server (transferTo)
 *   - read timeout (SO_TIMEOUT) ใช้ได้กับการอ่าน header เท่านั้น ส่วน transferFrom ไม่รองรับ timeout
 *     ถ้า server ค้างกลางทางในโหมด NIO worker จะรอจนกว่า connection จะถูกตัด
 *
 * Callable ไม่มี state ที่แก้ไขระหว่างทำงาน (ทุกอย่างเป็น final) ใช้ object หนึ่งตัวต่อหนึ่งช่วง
 */
public final class DownloadWorker implements Callable<Long>
{

    /** วิธีอ่าน/เขียนของ worker */
    public enum Mode
    {
        TRADITIONAL,
        NIO
    }

    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 30_000;

    private final String host;
    private final int port;
    private final String filename;
    private final Range range;
    private final Path target;
    private final long targetOffset;
    private final Mode mode;
    private final boolean ownsTarget; // true = part file ของ worker นี้เอง (ล้างได้) / false = ไฟล์ร่วม

    public DownloadWorker(String host, int port, String filename, Range range,
                            Path target, long targetOffset, Mode mode)
    {
        if (host == null || filename == null || range == null || target == null || mode == null)
        {
            throw new IllegalArgumentException("host, filename, range, target and mode must not be null");
        }
        if (targetOffset < 0)
        {
            throw new IllegalArgumentException("targetOffset must not be negative: " + targetOffset);
        }
        this.host = host;
        this.port = port;
        this.filename = filename;
        this.range = range;
        this.target = target;
        this.targetOffset = targetOffset;
        this.mode = mode;

        // แบบ parts: ชื่อไฟล์ปลายทางตรงกับชื่อ part ของช่วงนี้ และเขียนจากต้นไฟล์
        Path name = target.getFileName();
        this.ownsTarget = targetOffset == 0
                && name != null
                && name.toString().equals(range.partFileName(filename));
    }

    // ============================================================
    // จุดเข้าหลัก
    // ============================================================

    /** @return จำนวน byte ที่รับได้ (เท่ากับ range.length() เมื่อสำเร็จ) */
    @Override
    public Long call() throws IOException, ProtocolException
    {
        if (range.length() == 0)
        {
            if (ownsTarget)
            {
                openTargetChannel().close(); // สร้าง part file เปล่าไว้ให้ขั้น merge
            }
            return 0L;
        }
        return mode == Mode.NIO ? downloadNio() : downloadTraditional();
    }

    // ============================================================
    // Traditional: Socket + InputStream + RandomAccessFile
    // ============================================================

    private long downloadTraditional() throws IOException, ProtocolException
    {
        try (Socket socket = new Socket())
        {
            socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(READ_TIMEOUT_MS);
            socket.setTcpNoDelay(true);

            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream(); // ไม่ห่อ buffer ตามที่ Protocol.readLine ต้องการ

            out.write(requestBytes());
            out.flush();
            expectOk(Protocol.readLine(in));

            byte[] buffer = new byte[Protocol.BUFFER_SIZE];
            try (RandomAccessFile raf = new RandomAccessFile(target.toFile(), "rw"))
            {
                if (ownsTarget)
                {
                    raf.setLength(0);
                }
                raf.seek(targetOffset);

                long remaining = range.length();
                while (remaining > 0)
                {
                    int want = (int) Math.min(buffer.length, remaining);
                    int n = in.read(buffer, 0, want);
                    if (n == -1)
                    {
                        throw new IOException("server closed early: " + remaining
                                + " bytes missing in " + range);
                    }
                    raf.write(buffer, 0, n);
                    remaining -= n;
                }
            }
            return range.length();
        }
    }

    // ============================================================
    // NIO: SocketChannel + FileChannel.transferFrom
    // ============================================================

    private long downloadNio() throws IOException, ProtocolException
    {
        try (SocketChannel channel = SocketChannel.open())
        {
            // ใช้ adaptor socket() เพื่อ connect แบบมี timeout และอ่าน header ผ่าน InputStream
            // (ยังเป็น blocking mode ตามค่าเริ่มต้น และ InputStream ของ adaptor ไม่มี buffer ของตัวเอง
            //  จึงไม่กิน payload ที่ตามมา)
            Socket socket = channel.socket();
            socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(READ_TIMEOUT_MS);
            socket.setTcpNoDelay(true);

            ByteBuffer request = ByteBuffer.wrap(requestBytes());
            while (request.hasRemaining())
            {
                channel.write(request);
            }
            expectOk(Protocol.readLine(socket.getInputStream()));

            try (FileChannel file = openTargetChannel())
            {
                long position = targetOffset;
                long remaining = range.length();
                // transferFrom ไม่รับประกันว่าจะได้ครบในครั้งเดียว จึงต้องวนจนครบ
                while (remaining > 0)
                {
                    long n = file.transferFrom(channel, position, remaining);
                    if (n <= 0)
                    {
                        // channel เป็น blocking ถ้าได้ 0 แปลว่า server ปิด connection ก่อนส่งครบ
                        throw new IOException("server closed early: " + remaining
                                + " bytes missing in " + range);
                    }
                    position += n;
                    remaining -= n;
                }
            }
            return range.length();
        }
    }

    // ============================================================
    // ตัวช่วย
    // ============================================================

    private byte[] requestBytes()
    {
        return Protocol.getRequest(filename, range.offset(), range.length())
                .getBytes(StandardCharsets.UTF_8);
    }

    /** ตรวจ header ของ server: ถ้าเป็น ERROR โยน ProtocolException, ถ้าความยาวไม่ตรงที่ขอก็ถือว่าผิดพลาด */
    private void expectOk(String header) throws IOException, ProtocolException
    {
        long promised = Protocol.parseOkResponse(header);
        if (promised != range.length())
        {
            throw new IOException("server promised " + promised + " bytes but " + range + " was requested");
        }
    }

    /**
     * เปิด FileChannel ไปยังไฟล์ปลายทาง
     *   parts  : สร้างใหม่/ล้างไฟล์เก่า
     *   direct : ไฟล์ต้องมีอยู่แล้ว (DownloadClient จองขนาดไว้) เปิดแบบเขียนอย่างเดียว ไม่แตะเนื้อหาส่วนอื่น
     */
    private FileChannel openTargetChannel() throws IOException
    {
        if (ownsTarget)
        {
            return FileChannel.open(target, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        }
        return FileChannel.open(target, StandardOpenOption.WRITE);
    }
}