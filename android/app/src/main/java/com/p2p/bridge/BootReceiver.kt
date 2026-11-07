package com.p2p.bridge

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent

/**
 * Starts the bridge when the device boots (or the app is updated), if autostart is enabled.
 *
 * Note: Android only delivers BOOT_COMPLETED to apps that have been launched at least once by the
 * user (an app in the "stopped" state gets nothing), so open the app once after installing.
 */
class BootReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        when (intent.action) {
            Intent.ACTION_BOOT_COMPLETED,
            "android.intent.action.QUICKBOOT_POWERON",
            "com.htc.intent.action.QUICKBOOT_POWERON",
            Intent.ACTION_MY_PACKAGE_REPLACED -> {
                if (Prefs.autostart(context)) {
                    BridgeService.start(context)
                }
            }
        }
    }
}
