# Operating System Context

This file is injected into every runtime turn as part of always-on operating memory.

## Target Platform

- **Primary**: Windows 11 (x86-64)
- **Secondary**: macOS (Apple Silicon — Mac Studio, planned)

## Host Control Stack (Windows)

The Windows host backend binary (`splcw-host-backend`) provides:

- screenshot capture (GDI/WinRT)
- OCR (Windows.Media.Ocr)
- active window detection and window enumeration (Win32 API)
- window focus / move / resize
- mouse: move, click, double-click, right-click, drag, scroll
- keyboard: text input, hotkeys/chords
- clipboard read / write
- process launch, close, enumeration
- UI Automation (UIA) tree inspection

## Host Control Stack (macOS)

macOS backend via Swift sidecar provides:

- screenshot, OCR, window enumeration
- Quartz/Accessibility-backed pointer and keyboard input
- focused-control value proof via Accessibility APIs

## One-Body Rule

All host-body actions are serialized through a single lane. Planning may parallelize. Actuation does not.

## Verification Contract

Every host action must be followed by an observation that confirms world-state changed as intended. Command success alone is not sufficient proof. Structured verification signals (`PostActionVerificationSignal`) are the ground truth.

## Paths

- Repo root: detected at runtime by walking parent dirs for `.git/`
- Session data: `{repo_root}/artifacts/ultimentality-pilot/operator/`
- Offload mirror: `{repo_root}/offload/current/`
- Memory surfaces: `{repo_root}/artifacts/ultimentality-pilot/memory/`
