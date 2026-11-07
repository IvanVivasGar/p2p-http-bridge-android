plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "com.p2p.bridge"
    compileSdk = 34

    defaultConfig {
        applicationId = "com.p2p.bridge"
        // Matches the NDK API level used by core/build.sh (API=24).
        minSdk = 24
        // 30 on purpose: no foreground-service-type permissions (API 34) and, together with
        // nativeLibraryDir execution, no W^X surprises on Fire OS 7/8 (Android 9/11).
        targetSdk = 30
        versionCode = 1
        versionName = "1.0.0"

        // Only the ABIs the daemon is built for (Fire TV Stick Lite/Basic = v7a, 4K/4K Max = v8a).
        ndk { abiFilters += listOf("armeabi-v7a", "arm64-v8a") }
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            // Debug-signed so `make release` yields an installable APK for sideloading.
            // Replace with a real signingConfig for distribution.
            signingConfig = signingConfigs.getByName("debug")
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions { jvmTarget = "17" }

    packaging {
        jniLibs {
            // CRITICAL: libbridge.so is an executable, not a shared library. Legacy packaging makes
            // the installer extract it to nativeLibraryDir (the only exec-allowed location on
            // Android 10+), and keeps the strip task away from it.
            useLegacyPackaging = true
            keepDebugSymbols += "**/libbridge.so"
        }
    }

    lint { abortOnError = false }
}

dependencies {
    implementation("androidx.core:core-ktx:1.13.1")
}
