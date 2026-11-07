package com.p2p.bridge

import android.app.Activity
import android.graphics.Color
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.widget.Button
import android.widget.CheckBox
import android.widget.ScrollView
import android.widget.TextView
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL
import kotlin.concurrent.thread

/**
 * Minimal Android TV / Leanback-style diagnostics screen: start/stop, status, port selector,
 * autostart toggle, live daemon health and the live log. Fully operable with a D-pad remote.
 */
class MainActivity : Activity(), BridgeState.Listener {

    private lateinit var status: TextView
    private lateinit var stats: TextView
    private lateinit var btnToggle: Button
    private lateinit var portValue: TextView
    private lateinit var logText: TextView
    private lateinit var logScroll: ScrollView

    private val main = Handler(Looper.getMainLooper())
    private var port = Prefs.DEFAULT_PORT
    private var logLines = 0

    @Volatile private var polling = false
    private val applyPortRunnable = Runnable {
        // Port changed while the bridge is active: restart it on the new port.
        if (isActive()) BridgeService.start(this, port)
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        status = findViewById(R.id.status)
        stats = findViewById(R.id.stats)
        btnToggle = findViewById(R.id.btnToggle)
        portValue = findViewById(R.id.portValue)
        logText = findViewById(R.id.logText)
        logScroll = findViewById(R.id.logScroll)
        val chkAutostart = findViewById<CheckBox>(R.id.chkAutostart)
        val btnMinus = findViewById<Button>(R.id.btnPortMinus)
        val btnPlus = findViewById<Button>(R.id.btnPortPlus)

        port = Prefs.port(this)
        portValue.text = port.toString()
        chkAutostart.isChecked = Prefs.autostart(this)
        chkAutostart.setOnCheckedChangeListener { _, checked -> Prefs.setAutostart(this, checked) }

        btnToggle.setOnClickListener {
            if (isActive()) BridgeService.stop(this) else BridgeService.start(this, port)
        }

        // OK = ±1, long OK = ±100 (typing digits with a TV remote is painful).
        btnMinus.setOnClickListener { changePort(-1) }
        btnMinus.setOnLongClickListener { changePort(-100); true }
        btnPlus.setOnClickListener { changePort(+1) }
        btnPlus.setOnLongClickListener { changePort(+100); true }

        btnToggle.requestFocus()
        renderLogs()
        renderState()
    }

    override fun onStart() {
        super.onStart()
        BridgeState.addListener(this)
        renderLogs()
        renderState()
    }

    override fun onResume() {
        super.onResume()
        startHealthPolling()
    }

    override fun onPause() {
        polling = false
        super.onPause()
    }

    override fun onStop() {
        BridgeState.removeListener(this)
        main.removeCallbacks(applyPortRunnable)
        super.onStop()
    }

    // ---- BridgeState.Listener (main thread)

    override fun onStateChanged() = renderState()

    override fun onLog(line: String) {
        logLines++
        if (logLines > 600) { // keep the TextView small on a 1 GB device
            renderLogs()
            return
        }
        logText.append(line + "\n")
        scrollLogToEnd()
    }

    // ---- rendering

    private fun isActive(): Boolean = when (BridgeState.status) {
        BridgeState.Status.STARTING, BridgeState.Status.RUNNING, BridgeState.Status.RESTARTING -> true
        else -> false
    }

    private fun renderState() {
        val st = BridgeState.status
        val (text, color) = when (st) {
            BridgeState.Status.STOPPED -> getString(R.string.state_stopped) to R.color.text_dim
            BridgeState.Status.STARTING -> getString(R.string.state_starting) to R.color.warn
            BridgeState.Status.RUNNING -> getString(R.string.state_running, BridgeState.port) to R.color.ok
            BridgeState.Status.RESTARTING ->
                getString(R.string.state_restarting, BridgeState.detail.toIntOrNull() ?: 0) to R.color.warn
            BridgeState.Status.ERROR -> getString(R.string.state_error, BridgeState.detail) to R.color.err
        }
        status.text = text
        status.setTextColor(resources.getColor(color, theme))
        btnToggle.setText(if (isActive()) R.string.btn_stop else R.string.btn_start)
        if (st != BridgeState.Status.RUNNING) stats.text = ""
    }

    private fun renderLogs() {
        val lines = BridgeState.logSnapshot().takeLast(300)
        logLines = lines.size
        logText.text = lines.joinToString("\n", postfix = if (lines.isEmpty()) "" else "\n")
        scrollLogToEnd()
    }

    private fun scrollLogToEnd() {
        // scrollTo (not fullScroll) so D-pad focus isn't stolen.
        logScroll.post { logScroll.scrollTo(0, logText.height) }
    }

    private fun changePort(delta: Int) {
        port = (port + delta).coerceIn(1024, 65535)
        Prefs.setPort(this, port)
        portValue.text = port.toString()
        main.removeCallbacks(applyPortRunnable)
        main.postDelayed(applyPortRunnable, 900) // debounce rapid presses
    }

    // ---- live /health

    private fun startHealthPolling() {
        if (polling) return
        polling = true
        thread(name = "health-poll", isDaemon = true) {
            while (polling) {
                val text = if (BridgeState.status == BridgeState.Status.RUNNING) fetchHealth(BridgeState.port) else null
                if (text != null) main.post { if (BridgeState.status == BridgeState.Status.RUNNING) stats.text = text }
                try { Thread.sleep(3000) } catch (_: InterruptedException) { return@thread }
            }
        }
    }

    private fun fetchHealth(port: Int): String? = try {
        val c = URL("http://127.0.0.1:$port/health").openConnection() as HttpURLConnection
        c.connectTimeout = 1500
        c.readTimeout = 1500
        val body = try { c.inputStream.bufferedReader().readText() } finally { c.disconnect() }
        val j = JSONObject(body)
        val mem = j.getJSONObject("memory")
        val cache = j.getJSONObject("cache")
        val torrents = j.optJSONArray("torrents")
        var peers = 0
        var name = ""
        if (torrents != null && torrents.length() > 0) {
            val t = torrents.getJSONObject(0)
            peers = t.optInt("activePeers")
            name = t.optString("name")
        }
        "RAM Go: %.0f MB · Caché: %.0f/%d MB · Peers: %d · Seeds: %d%s".format(
            mem.optDouble("goSysMB"), cache.optDouble("usedMB"), cache.optInt("budgetMB"),
            peers, j.optInt("seedsTotal"), if (name.isNotEmpty()) " · $name" else ""
        )
    } catch (_: Exception) {
        null
    }
}
