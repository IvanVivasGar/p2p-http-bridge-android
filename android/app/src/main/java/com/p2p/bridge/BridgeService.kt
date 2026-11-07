package com.p2p.bridge

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import androidx.core.app.NotificationCompat
import androidx.core.content.ContextCompat
import java.util.concurrent.Executors

/**
 * Foreground service that keeps the Go daemon alive.
 *
 * Supervision:
 *  - If the process dies on its own (crash, killed by the kernel/OOM killer) it is restarted with
 *    exponential backoff (1, 2, 4 ... 30 s). The backoff resets after a run of >= 60 s.
 *  - 5 consecutive crashes within 5 s of starting (e.g. port already in use) put the service in
 *    ERROR instead of looping forever; pressing Start again retries.
 *  - If the whole app process is killed, START_STICKY makes Android re-create the service, which
 *    starts the daemon again (the daemon itself exits when our stdin pipe closes, so no orphan
 *    keeps the port busy).
 */
class BridgeService : Service(), ProcessManager.Callback {

    companion object {
        const val ACTION_START = "com.p2p.bridge.START"
        const val ACTION_STOP = "com.p2p.bridge.STOP"
        const val EXTRA_PORT = "port"

        private const val CHANNEL_ID = "bridge"
        private const val NOTIF_ID = 1
        private const val MAX_FAST_FAILURES = 5
        private const val FAST_FAILURE_MS = 5_000L
        private const val STABLE_RUN_MS = 60_000L

        /** Starts (or re-starts on another port) the service. Safe from boot receivers. */
        fun start(ctx: Context, port: Int = Prefs.port(ctx)) {
            val i = Intent(ctx, BridgeService::class.java).setAction(ACTION_START).putExtra(EXTRA_PORT, port)
            ContextCompat.startForegroundService(ctx, i)
        }

        fun stop(ctx: Context) {
            ctx.startService(Intent(ctx, BridgeService::class.java).setAction(ACTION_STOP))
        }
    }

    private lateinit var pm: ProcessManager
    private val main = Handler(Looper.getMainLooper())
    private val worker = Executors.newSingleThreadExecutor()

    private var port = Prefs.DEFAULT_PORT
    private var userStopped = false
    private var destroyed = false
    private var restartAttempts = 0
    private var fastFailures = 0

    override fun onCreate() {
        super.onCreate()
        pm = ProcessManager(this, this)
        createChannel()
        // Must be called within 5 s of startForegroundService().
        startForeground(NOTIF_ID, buildNotification(getString(R.string.state_starting)))
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        when (intent?.action) {
            ACTION_STOP -> {
                userStopped = true
                main.removeCallbacksAndMessages(null)
                worker.execute {
                    pm.stop()
                    main.post {
                        BridgeState.setState(BridgeState.Status.STOPPED, port)
                        BridgeState.appendLog("[bridge] detenido")
                        stopForeground(true)
                        stopSelf()
                    }
                }
                return START_NOT_STICKY
            }
            else -> {
                // ACTION_START, or a null intent after the system re-created us (START_STICKY).
                val requested = intent?.getIntExtra(EXTRA_PORT, -1)?.takeIf { it > 0 } ?: Prefs.port(this)
                val portChanged = requested != port
                port = requested
                userStopped = false
                restartAttempts = 0
                fastFailures = 0
                main.removeCallbacksAndMessages(null)
                worker.execute {
                    if (portChanged && pm.isRunning()) pm.stop()
                    launch()
                }
                return START_STICKY
            }
        }
    }

    /** Runs on the worker thread. */
    private fun launch() {
        if (userStopped || destroyed || pm.isRunning()) return
        BridgeState.setState(BridgeState.Status.STARTING, port)
        updateNotification(getString(R.string.state_starting))
        try {
            pm.start(port)
        } catch (e: Exception) {
            val msg = e.message ?: e.javaClass.simpleName
            BridgeState.appendLog("[bridge] no se pudo iniciar: $msg")
            BridgeState.setState(BridgeState.Status.ERROR, port, msg)
            updateNotification(getString(R.string.state_error, msg))
        }
    }

    // ---- ProcessManager.Callback (background threads)

    override fun onLine(line: String) {
        BridgeState.appendLog(line)
        // The daemon logs: level=INFO msg=listening addr=127.0.0.1:8080 ...
        if (line.contains("msg=listening")) {
            BridgeState.setState(BridgeState.Status.RUNNING, port)
            updateNotification(getString(R.string.state_running, port))
        }
    }

    override fun onExit(exitCode: Int, uptimeMs: Long) {
        main.post {
            if (userStopped || destroyed) return@post
            BridgeState.appendLog("[bridge] el proceso terminó (código $exitCode) tras ${uptimeMs / 1000} s")

            fastFailures = if (uptimeMs < FAST_FAILURE_MS) fastFailures + 1 else 0
            if (uptimeMs >= STABLE_RUN_MS) restartAttempts = 0

            if (fastFailures >= MAX_FAST_FAILURES) {
                val msg = "el daemon falla al arrancar (¿puerto $port ocupado?)"
                BridgeState.setState(BridgeState.Status.ERROR, port, msg)
                updateNotification(getString(R.string.state_error, msg))
                return@post
            }

            val delaySec = minOf(30, 1 shl minOf(restartAttempts, 5))
            restartAttempts++
            BridgeState.setState(BridgeState.Status.RESTARTING, port, delaySec.toString())
            updateNotification(getString(R.string.state_restarting, delaySec))
            BridgeState.appendLog("[bridge] reiniciando en $delaySec s…")
            main.postDelayed({ worker.execute { launch() } }, delaySec * 1000L)
        }
    }

    override fun onTaskRemoved(rootIntent: Intent?) {
        // Keep running when the UI is swiped away.
        super.onTaskRemoved(rootIntent)
    }

    override fun onDestroy() {
        destroyed = true
        main.removeCallbacksAndMessages(null)
        // Stop synchronously-ish: the executor is single-threaded so this runs after any launch().
        worker.execute { pm.stop() }
        worker.shutdown()
        if (BridgeState.status != BridgeState.Status.ERROR) {
            BridgeState.setState(BridgeState.Status.STOPPED, port)
        }
        super.onDestroy()
    }

    // ---- notification

    private fun createChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val ch = NotificationChannel(CHANNEL_ID, getString(R.string.notif_channel), NotificationManager.IMPORTANCE_LOW)
            getSystemService(NotificationManager::class.java).createNotificationChannel(ch)
        }
    }

    private fun buildNotification(text: String): Notification {
        val flags = PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        val open = PendingIntent.getActivity(this, 0, Intent(this, MainActivity::class.java), flags)
        val stop = PendingIntent.getService(
            this, 1, Intent(this, BridgeService::class.java).setAction(ACTION_STOP), flags
        )
        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_notification)
            .setContentTitle(getString(R.string.notif_title))
            .setContentText(text)
            .setContentIntent(open)
            .addAction(0, getString(R.string.notif_stop), stop)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()
    }

    private fun updateNotification(text: String) {
        getSystemService(NotificationManager::class.java).notify(NOTIF_ID, buildNotification(text))
    }
}
