#![deny(unused)]
#![deny(clippy::panic, clippy::unwrap_used, clippy::expect_used)]
use std::{
    path::Path,
    sync::{Arc, atomic::AtomicBool},
};

use crossfire::{AsyncRx, spsc::Array};
use ffmpeg_the_third::{
    Rational,
    color::{Space, TransferCharacteristic},
    format::Pixel,
    frame::{Audio, Video},
};
use tokio::{
    runtime::Handle,
    sync::{RwLock, RwLockReadGuard},
};

use crate::decode_engine::{TinyDecoder, TinyDecoderArgs};
mod decode_engine;
pub(crate) type PlayerResult<T> = anyhow::Result<T>;
/// Main type for high level interface.
pub struct MediaEngine {
    tiny_decoder: RwLock<TinyDecoder>,
    media_source_info: RwLock<Option<MediaSourceInfo>>,
    pub realtime_status: RealtimeStatus,
}
impl MediaEngine {
    /// Construct an engine without specifing a source.
    pub fn new(handle: Handle) -> anyhow::Result<Arc<Self>> {
        let media_source_flag = Arc::new(AtomicBool::new(false));
        let hardware_config_flag = Arc::new(AtomicBool::new(false));
        let demux_eof_flag = Arc::new(AtomicBool::new(false));
        let tiny_decoder_creation_args = TinyDecoderArgs::builder()
            .runtime_handle(handle)
            .media_source_flag(media_source_flag.clone())
            .hardware_config_flag(hardware_config_flag.clone())
            .demux_eof_flag(demux_eof_flag.clone())
            .build();
        let tiny_decoder = RwLock::new(TinyDecoder::new(tiny_decoder_creation_args)?);
        let realtime_status = RealtimeStatus {
            media_source_flag,
            demux_eof_flag,
            hardware_config_flag,
        };
        Ok(Arc::new(Self {
            tiny_decoder,
            media_source_info: RwLock::new(None),
            realtime_status,
        }))
    }
    /// Attempting retrieving the source information.
    pub fn try_borrow_media_source_info(
        &self,
    ) -> anyhow::Result<RwLockReadGuard<'_, Option<MediaSourceInfo>>> {
        self.media_source_info
            .try_read()
            .map_err(anyhow::Error::from)
    }
    /// Retrieve the source information.
    pub async fn media_source_info(&self) -> RwLockReadGuard<'_, Option<MediaSourceInfo>> {
        self.media_source_info.read().await
    }
    /// Specify an input source.
    pub async fn reset_input(&self, path: &Path) -> anyhow::Result<FrameReceivers> {
        let mut decoder = self.tiny_decoder.write().await;
        if let Ok((info, recvs)) = decoder.reset_input(path).await {
            *self.media_source_info.write().await = Some(info);
            Ok(recvs)
        } else {
            Err(anyhow::Error::msg("reset input err"))
        }
    }
    /// Seek a specified time by timestamp.
    pub async fn seek_timestamp(&self, ts: i64) {
        let decoder = self.tiny_decoder.read().await;
        decoder.seek_timestamp_to_decode(ts).await;
    }
}
/// indicate which stream in the input is chosen as main stream
/// always prefer Audio Stream and fallback to Video Stream
#[derive(Debug, Clone)]
pub struct StreamExistenceFlags {
    pub video: bool,
    pub audio: bool,
}
/// For constructing gpu transcoders.
#[derive(Clone)]
pub struct TranscoderArgs {
    pub colorspace: Space,
    pub pixel_format: Pixel,
    pub transfer_characteristic: TransferCharacteristic,
    pub width: u32,
    pub height: u32,
}
/// Shared realtime states.
pub struct RealtimeStatus {
    pub media_source_flag: Arc<AtomicBool>,
    pub demux_eof_flag: Arc<AtomicBool>,
    pub hardware_config_flag: Arc<AtomicBool>,
}

/// Souce information, only exist after calling reset_input.
#[derive(Clone)]
pub struct MediaSourceInfo {
    pub transcoder_args: Option<TranscoderArgs>,
    pub stream_existence_flags: StreamExistenceFlags,
    pub end_timestamp: i64,
    pub end_time_formatted_str: Box<str>,
    pub cover_pic_data: Option<Box<[u8]>>,
    pub resolution_rect: [u32; 2],
    pub audio_time_base: Rational,
    pub video_time_base: Rational,
}
pub struct FrameReceivers {
    pub audio_frame_recv: AsyncRx<Array<Audio>>,
    pub video_frame_recv: AsyncRx<Array<Video>>,
}
#[allow(unused)]
mod test {
    use std::path::Path;

    use tokio::task::JoinHandle;

    use crate::MediaEngine;

    #[test]
    fn hello_world() {
        let async_runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let handle = async_runtime.handle().clone();

        let res: anyhow::Result<()> = async_runtime.block_on(async move {
            let media_path = "world.mkv";
            let media_engine = MediaEngine::new(handle)?;
            let recvs = media_engine.reset_input(Path::new(media_path)).await?;

            let audio_frame = recvs.audio_frame_recv.recv().await?;
            let video_frame = recvs.video_frame_recv.recv().await?;
            anyhow::Result::Ok(())
        });
    }
}
