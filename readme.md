# Getting Started

This crate provides high-level interfaces for building media players.

For compilation setup, see [ffmpeg-the-third](https://crates.io/crates/ffmpeg-the-third).

`MediaEngine` requires a [Tokio](https://crates.io/crates/tokio) runtime handle:

```rust
let media_path = "world.mkv";

let async_runtime = tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .build()?;

let handle = async_runtime.handle().clone();
let media_engine = MediaEngine::new(handle)?;

media_engine.reset_input(media_path).await?;

let audio_frame = media_engine.realtime_status.audio_frame_recv.recv_async().await?;
let video_frame = media_engine.realtime_status.video_frame_recv.recv_async().await?;