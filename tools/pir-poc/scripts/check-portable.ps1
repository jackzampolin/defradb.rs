$ErrorActionPreference = "Stop"

# Build-only Windows portability gate. The repository currently has a known
# upstream sha2-asm/MSVC blocker; this script must remain red until it is fixed
# or the client-only crate removes that native server dependency.

$repo = Resolve-Path (Join-Path $PSScriptRoot "..\..\..")
Push-Location $repo
try {
    $target = "x86_64-pc-windows-msvc"
    $toolchain = if ($env:PIR_RUST_TOOLCHAIN) {
        $env:PIR_RUST_TOOLCHAIN
    }
    else {
        "1.91.0-x86_64-pc-windows-msvc"
    }
    $installedToolchains = rustup toolchain list
    if (-not ($installedToolchains | Select-String -SimpleMatch $toolchain)) {
        throw "Pinned Rust toolchain is not installed: $toolchain"
    }
    $installed = rustup target list --installed
    if ($installed -notcontains $target) {
        Write-Output "PORTABLE_BUILD target=$target status=not-installed"
        if ($env:PIR_REQUIRE_PORTABLE_TARGETS -eq "1") {
            throw "Required portability target is not installed: $target"
        }
        exit 0
    }

    Write-Output "PORTABLE_BUILD target=$target toolchain=$toolchain status=checking"
    rustup run $toolchain cargo check -p pir-poc --lib --target $target
    if ($LASTEXITCODE -ne 0) {
        throw "PORTABLE_BUILD target=$target status=failed evidence=build-only"
    }
    Write-Output "PORTABLE_BUILD target=$target status=passed evidence=build-only"
    Write-Output "PORTABLE_BUILD reminder='build pass is not a device performance or energy result'"
}
finally {
    Pop-Location
}
