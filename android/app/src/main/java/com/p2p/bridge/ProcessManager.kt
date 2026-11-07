package com.p2p.bridge

import android.content.Context
import android.os.Build
import android.os.Handler
import android.os.Looper
import java.io.File
import java.io.FileOutputStream
import java.util.concurrent.CopyOnWriteArraySet
import java.util.concurrent.atomic.AtomicBoolean

/** Persistent settings (port, autostart). */
object Prefs {
    private const val FILE = "bridge"
    private const val KEY_PORT = "port"
    private const val KEY_AUTOSTART = "autostart"
    const val DEFAULT_PORT = 8080

    private fun sp(ctx: Context) = ctx.applicationContext.getSharedPreferences(FILE, Context.MODE_PRIVATE)

    fun port(ctx: Context): Int = sp(ctx).getInt(KEY_PORT, DEFAULT_PORT).coerceIn(1024, 65535)
    fun setPort(ctx: Context, port: Int) = sp(ctx).edit().putInt(KEY_PORT, port.coerceIn(1024, 65535)).apply()

    /** Start automatically when the device boots. On by default. */
    fun autostart(ctx: Context): Boolean = sp(ctx).getBoolean(KEY_AUTOSTART, true)
    fun setAutostart(ctx: Context, v: Boolean) = sp(ctx).edit().putBoolean(KEY_AUTOSTART, v).apply()
}

/**
 * Process-wide state shared by [BridgeService] (writer) and [MainActivity] (reader). Both live in
 * the same process, so a plain singleton is enough. Listener callbacks always run on the main thread.
 */
object BridgeState {
    enum class Status { STOPPED, STARTING, RUNNING, RESTARTING, ERROR }

    interface Listener {
        fun onStateChanged()
        fun onLog(line: String)
    }

    private const val MAX_LOG_LINES = 500
    private val main = Handler(Looper.getMainLooper())
    private val listeners = CopyOnWriteArraySet<Listener>()
    private val logs = ArrayDeque<String>()

    @Volatile var status: Status = Status.STOPPED; private set
    @Volatile var port: Int = Prefs.DEFAULT_PORT; private set
    @Volatile var detail: String = ""; private set

    fun addListener(l: Listener) { listeners.add(l) }
    fun removeListener(l: Listener) { listeners.remove(l) }

    fun logSnapshot(): List<String> = synchronized(logs) { logs.toList() }

    fun setState(status: Status, port: Int = this.port, detail: String = "") {
        this.status = status
        this.port = port
        this.detail = detail
        main.post { listeners.forEach { it.onStateChanged() } }
    }

    fun appendLog(line: String) {
        synchronized(logs) {
            logs.addLast(line)
            while (logs.size > MAX_LOG_LINES) logs.removeFirst()
        }
        main.post { listeners.forEach { it.onLog(line) } }
    }
}

/**
 * Owns the Go daemon child process: resolves/extracts the binary, spawns it with [ProcessBuilder],
 * pumps its merged stdout/stderr to [Callback.onLine] and reports unexpected exits.
 * Supervision (restart policy) lives in [BridgeService].
 */
class ProcessManager(private val context: Context, private val callback: Callback) {

    interface Callback {
        /** Called from a background thread for each output line. */
        fun onLine(line: String)

        /** Called from a background thread when the process died WITHOUT [stop] being called. */
        fun onExit(exitCode: Int, uptimeMs: Long)
    }

    private class Running(val process: Process, val intentionalStop: AtomicBoolean, val startedAt: Long)

    @Volatile private var running: Running? = null

    fun isRunning(): Boolean = running?.process?.let(::alive) == true

    /** Starts the daemon. Throws if the binary can't be found/executed. */
    @Synchronized
    fun start(port: Int, cacheMb: Int = 64) {
        if (isRunning()) return
        val binary = resolveBinary()
        callback.onLine("[bridge] binario: ${binary.absolutePath}")

        val cmd = listOf(
            binary.absolutePath,
            "-host", "127.0.0.1",
            "-port", port.toString(),
            "-cache-mb", cacheMb.toString(),
            // The daemon exits when our stdin pipe closes, i.e. when this app process dies
            // (e.g. killed by the low-memory killer). Prevents orphans holding the port.
            "-watch-stdin",
        )
        val pb = ProcessBuilder(cmd)
            .directory(context.filesDir)
            .redirectErrorStream(true) // stdout + stderr in one stream
        pb.environment().apply {
            put("HOME", context.filesDir.absolutePath)
            put("TMPDIR", context.cacheDir.absolutePath)
        }

        val process = pb.start()
        val r = Running(process, AtomicBoolean(false), System.currentTimeMillis())
        running = r

        Thread({
            try {
                process.inputStream.bufferedReader().forEachLine { callback.onLine(it) }
            } catch (_: Exception) {
                // stream closed on process death
            }
        }, "bridge-stdout").apply { isDaemon = true }.start()

        Thread({
            val code = try { process.waitFor() } catch (_: InterruptedException) { -1 }
            if (running === r) running = null
            if (!r.intentionalStop.get()) {
                callback.onExit(code, System.currentTimeMillis() - r.startedAt)
            }
        }, "bridge-waiter").apply { isDaemon = true }.start()
    }

    /** Stops the daemon (no [Callback.onExit] is delivered for this exit). */
    @Synchronized
    fun stop() {
        val r = running ?: return
        r.intentionalStop.set(true)
        r.process.destroy()
        // Process.waitFor(timeout)/isAlive need API 26; poll for minSdk 24.
        val deadline = System.currentTimeMillis() + 2000
        while (alive(r.process) && System.currentTimeMillis() < deadline) {
            try { Thread.sleep(50) } catch (_: InterruptedException) { break }
        }
        running = null
    }

    private fun alive(p: Process): Boolean = try { p.exitValue(); false } catch (_: IllegalThreadStateException) { true }

    /**
     * Preferred: `libbridge.so` in nativeLibraryDir. The system extracts it at install time
     * (extractNativeLibs + useLegacyPackaging) and it is the only location from which an app may
     * exec files on Android 10+. Fallback: extract `assets/bridge/<abi>/bridge` into
     * `filesDir/bin` and chmod 755 (works on Android 9 / targetSdk < 29 only).
     */
    private fun resolveBinary(): File {
        val lib = File(context.applicationInfo.nativeLibraryDir, "libbridge.so")
        if (lib.isFile) {
            if (!lib.canExecute()) lib.setExecutable(true, false)
            return lib
        }

        for (abi in Build.SUPPORTED_ABIS) {
            val assetPath = "bridge/$abi/bridge"
            val dest = File(File(context.filesDir, "bin").apply { mkdirs() }, "bridge-$abi")
            try {
                context.assets.open(assetPath).use { input ->
                    // Re-extract only if missing or its size changed (app update).
                    val size = input.available().toLong()
                    if (!dest.isFile || dest.length() != size) {
                        FileOutputStream(dest).use { out -> input.copyTo(out) }
                    }
                }
            } catch (_: java.io.FileNotFoundException) {
                continue // no asset for this ABI
            }
            chmod755(dest)
            return dest
        }
        throw IllegalStateException(
            "No se encontró libbridge.so en ${context.applicationInfo.nativeLibraryDir} " +
                "ni assets/bridge/<abi>/bridge (ABIs: ${Build.SUPPORTED_ABIS.joinToString()}). " +
                "Ejecuta 'make daemon' antes de compilar el APK."
        )
    }

    private fun chmod755(f: File) {
        try {
            android.system.Os.chmod(f.absolutePath, 493) // 0755
        } catch (_: Exception) {
            f.setReadable(true, false)
            f.setExecutable(true, false)
        }
    }
}
