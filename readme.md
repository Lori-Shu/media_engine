# Getting Started

This crate provides high-level interfaces for building media players.

For compilation setup, see [ffmpeg-the-third](https://crates.io/crates/ffmpeg-the-third).

`MediaEngine` requires a [Tokio](https://crates.io/crates/tokio) runtime handle:

```rust
let async_runtime = 
tokio::runtime::Builder::new_multi_thread()
.enable_all()
.build()
.unwrap();

let handle = async_runtime.handle().clone();

let res: anyhow::Result<()> = async_runtime.block_on(
async move {
    let media_path = "world.mkv";
    let media_engine = MediaEngine::new(handle)?;
    let recvs = media_engine
    .reset_input(Path::new(media_path)).await?;

    let audio_frame = recvs.audio_frame_recv.recv().await?;
    let video_frame = recvs.video_frame_recv.recv().await?;
    anyhow::Result::Ok(())
});