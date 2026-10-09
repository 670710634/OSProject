package server;

import common.Protocol;
import common.Protocol.FileInfo;
import common.Protocol.ProtocolException;
import common.Protocol.Request;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * ClientHandler.java
 * ------------------------------------------------------------
 * ดูแล "หนึ่ง connection" จาก client หนึ่งตัว: อ่านคำสั่งทีละบรรทัด แล้วตอบ
 *   LIST                          -> FILE <name> <size> ... END
 *   INFO <filename>               -> SIZE <bytes>
 *   GET <filename> <offset> <len> -> OK <len>\n + payload ดิบ <len> byte
 *   ผิดพลาด                       -> ERROR <code> <message>
 *
 * connection หนึ่งรับได้หลายคำสั่งต่อเนื่อง (client ส่งคำสั่งถัดไปหลังได้คำตอบครบแล้ว)
 * จนกว่า client จะปิด connection หรือเงียบเกิน IDLE_TIMEOUT_MS
 *
 * สองโหมดต่างกันเฉพาะ "ตอนส่ง payload ของ GET" (ส่วนที่ benchmark ต้องการเทียบ)
 *   Traditional : RandomAccessFile.seek() + read() ลง byte[] แล้ว OutputStream.write()
 *                 ข้อมูลวิ่ง ดิสก์ -> kernel -> heap ของ Java -> kernel -> เครือข่าย
 *   NIO         : FileChannel.transferTo(offset, length, socketChannel)
 *                 ให้ kernel ส่งจากไฟล์เข้า socket ตรง ๆ (zero-copy) ไม่ผ่าน heap
 *
 * เลือกโหมดจาก constructor ที่ใช้: Socket = traditional, SocketChannel = nio
 *
 * หมายเหตุ: เมื่อส่ง header "OK <len>" ไปแล้ว ถ้าอ่านไฟล์พังกลางทางจะส่ง ERROR ไม่ได้อีก
 * (client กำลังรอ byte ดิบอยู่) จึงทำได้อย่างเดียวคือปิด connection
 * client เห็น stream จบก่อนครบ <len> ก็จะรู้ว่าล้มเหลว
 *
 * เปิด log ทุกคำสั่งด้วย  java -Dserver.verbose=true ...  (ปิดไว้เป็นค่าเริ่มต้น ไม่ให้กระทบเวลา benchmark)
 *
 * สัญญาที่ FileStore ต้องมี (ไฟล์ถัดไปที่จะเขียน)
 *   List<Protocol.FileInfo> list() throws IOException
 *   Path resolve(String filename) throws ProtocolException
 *       คืน path จริงของไฟล์ปกติใน root เท่านั้น ถ้าไม่พบ/ชื่อไม่ปลอดภัย (../) โยน ProtocolException
 */
public final class ClientHandler implements Runnable 
{

    private static final int IDLE_TIMEOUT_MS = 30_000;  // ไม่ส่งคำสั่งมานานเกินนี้ให้ตัด connection
    private static final boolean VERBOSE = Boolean.getBoolean("server.verbose");

    private final FileStore store;
    private final Socket socket;           // ใช้เมื่อโหมด traditional (อีกตัวเป็น null)
    private final SocketChannel channel;   // ใช้เมื่อโหมด nio (อีกตัวเป็น null)
    private final boolean nio;

    /** โหมด traditional: รับ Socket ธรรมดา */
    public ClientHandler(Socket socket, FileStore store) 
    {
        this.socket = socket;
        this.channel = null;
        this.store = store;
        this.nio = false;
    }

    /** โหมด nio: รับ SocketChannel (ต้องอยู่ใน blocking mode ซึ่งเป็นค่าเริ่มต้นของ accept()) */
    public ClientHandler(SocketChannel channel, FileStore store) 
    {
        this.socket = null;
        this.channel = channel;
        this.store = store;
        this.nio = true;
    }

    // ============================================================
    // วนอ่านคำสั่งของ connection นี้
    // ============================================================

    @Override
    public void run() 
    {
        String remote = "?";
        try 
        {
            // ทั้งสองโหมดเข้าถึง Socket ได้ (channel.socket() เป็น adaptor ที่ใช้ timeout ได้)
            Socket sock = nio ? channel.socket() : socket;
            remote = String.valueOf(sock.getRemoteSocketAddress());
            sock.setSoTimeout(IDLE_TIMEOUT_MS);
            sock.setTcpNoDelay(true); // header เล็ก ๆ ไม่ต้องรอ Nagle

            // ต้องเป็น stream ดิบที่ไม่ห่อ buffer (ดูเหตุผลที่ Protocol.readLine)
            InputStream in = sock.getInputStream();
            OutputStream out = nio ? null : sock.getOutputStream();

            while (true) 
            {
                String line = Protocol.readLine(in);
                if (line == null) 
                {
                    break; // client ปิด connection เรียบร้อย
                }
                handleLine(line, out, remote);
            }
        } 
        catch (IOException e) 
        {
            // client ตัดการเชื่อมต่อกลางคัน หรือ timeout เป็นเรื่องปกติ ไม่ต้องตกใจ
            log(remote, "closed: " + e.getMessage());
        } 
        finally 
        {
            closeQuietly();
        }
    }

    /** ประมวลผลหนึ่งบรรทัดคำสั่ง; error ของ protocol ตอบกลับเป็น ERROR แล้วรับคำสั่งต่อไปได้ */
    private void handleLine(String line, OutputStream out, String remote) throws IOException 
    {
        try 
        {
            Request req = Protocol.parseRequest(line);
            log(remote, line);

            switch (req.command()) 
            {
                case Protocol.CMD_LIST:
                    handleList(out);
                    break;
                case Protocol.CMD_INFO:
                    handleInfo(req, out);
                    break;
                case Protocol.CMD_GET:
                    handleGet(req, out);
                    break;
                default:
                    throw new ProtocolException(Protocol.ERR_BAD_REQUEST, "unknown command");
            }
        } 
        catch (ProtocolException e) 
        {
            send(out, Protocol.errorResponse(e.getCode(), e.getMessage()));
        }
    }

    // ============================================================
    // LIST / INFO
    // ============================================================

    private void handleList(OutputStream out) throws IOException 
    {
        List<FileInfo> files = store.list();
        StringBuilder sb = new StringBuilder();
        for (FileInfo f : files) 
        {
            sb.append(Protocol.fileLine(f.name(), f.size()));
        }
        sb.append(Protocol.endLine());
        send(out, sb.toString()); // ส่งทีเดียวทั้งก้อน ลดจำนวนครั้งที่เขียน socket
    }

    private void handleInfo(Request req, OutputStream out) throws IOException, ProtocolException 
    {
        Path file = store.resolve(req.filename());
        send(out, Protocol.sizeResponse(Files.size(file)));
    }

    // ============================================================
    // GET
    // ============================================================

    private void handleGet(Request req, OutputStream out) throws IOException, ProtocolException 
    {
        Path file = store.resolve(req.filename());
        long size = Files.size(file);
        long offset = req.offset();
        long length = req.length();

        // เขียนแบบนี้แทน offset + length > size เพื่อกัน long overflow ถ้า client ส่งเลขใหญ่มาก
        if (offset > size || length > size - offset) 
        {
            throw new ProtocolException(Protocol.ERR_BAD_RANGE,
                    "range [" + offset + ", +" + length + ") exceeds file size " + size);
        }

        send(out, Protocol.okResponse(length)); // header ก่อน แล้วตามด้วย payload ดิบ

        if (length == 0) 
        {
            return; // ขอ 0 byte ตอบ "OK 0" แล้วจบ
        }
        if (nio) 
        {
            sendRangeNio(file, offset, length);
        } 
        else 
        {
            sendRangeTraditional(file, offset, length, out);
        }
    }

    /** Traditional: seek -> read ลง buffer -> write ลง OutputStream ทีละ BUFFER_SIZE */
    private void sendRangeTraditional(Path file, long offset, long length, OutputStream out) throws IOException 
    {
        byte[] buffer = new byte[Protocol.BUFFER_SIZE];
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) 
        {
            raf.seek(offset);
            long remaining = length;
            while (remaining > 0) 
            {
                int want = (int) Math.min(buffer.length, remaining);
                int n = raf.read(buffer, 0, want);
                if (n == -1) 
                {
                    throw new IOException("file shrank while sending: " + file);
                }
                out.write(buffer, 0, n);
                remaining -= n;
            }
            out.flush();
        }
    }

    /** NIO: FileChannel.transferTo ส่งจากไฟล์เข้า socket ตรง ๆ (zero-copy) */
    private void sendRangeNio(Path file, long offset, long length) throws IOException 
    {
        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) 
        {
            long position = offset;
            long remaining = length;
            // transferTo ไม่รับประกันว่าจะส่งครบในครั้งเดียว (บางระบบจำกัดต่อครั้ง) จึงต้องวนจนครบ
            while (remaining > 0) 
            {
                long n = fc.transferTo(position, remaining, channel);
                if (n <= 0) 
                {
                    throw new IOException("transferTo made no progress at position " + position);
                }
                position += n;
                remaining -= n;
            }
        }
    }

    // ============================================================
    // ตัวช่วยส่งข้อความ / log / ปิด connection
    // ============================================================

    /** ส่งข้อความ (header/ERROR/LIST) ผ่านช่องทางของโหมดนั้น ๆ */
    private void send(OutputStream out, String text) throws IOException 
    {
        byte[] data = text.getBytes(StandardCharsets.UTF_8);
        if (nio) 
        {
            ByteBuffer buf = ByteBuffer.wrap(data);
            while (buf.hasRemaining()) 
            {
                channel.write(buf);
            }
        } 
        else 
        {
            out.write(data);
            out.flush();
        }
    }

    private void log(String remote, String message) 
    {
        if (VERBOSE) 
        {
            System.out.println("[" + remote + "] " + message);
        }
    }

    private void closeQuietly() 
    {
        try 
        {
            if (nio) 
            {
                channel.close();
            } 
            else 
            {
                socket.close();
            }
        } 
        catch (IOException ignored) 
        {
            // ปิดไม่ได้ก็ไม่มีอะไรให้ทำต่อ
        }
    }
}
