use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64},
    },
};

use ffmpeg_the_third::{
    Rational,
    color::{Space, TransferCharacteristic},
    format::Pixel,
    frame::{Audio, Video},
};
use flume::Receiver;
use tokio::{
    runtime::Handle,
    sync::{Notify, RwLock},
};

use crate::decode_engine::{TinyDecoder, TinyDecoderArgs};
mod decode_engine;
pub(crate) type PlayerResult<T> = anyhow::Result<T>;
/// Main type for high level interface.
pub struct MediaEngine {
    tiny_decoder: RwLock<TinyDecoder>,
    media_source_info: RwLock<Option<MediaSourceInfo>>,
    pub realtime_status: RealtimeStatus,
    pub background_tasks_notifies: BackgroundTasksNotifies,
}
impl MediaEngine {
    /// Construct an engine without specifing a source.
    pub fn new(handle: Handle) -> anyhow::Result<Arc<Self>> {
        let media_source_flag = Arc::new(AtomicBool::new(false));
        let hardware_config_flag = Arc::new(AtomicBool::new(false));
        let audio_frame_cache_queue = flume::bounded(32);
        let video_frame_cache_queue = flume::bounded(32);
        let audio_decode_thread_notify = Arc::new(Notify::new());
        let video_decode_thread_notify = Arc::new(Notify::new());
        let current_video_timestamp = Arc::new(AtomicI64::new(0));
        let demux_eof_flag = Arc::new(AtomicBool::new(false));
        let tiny_decoder_creation_args = TinyDecoderArgs::builder()
            .runtime_handle(handle)
            .media_source_flag(media_source_flag.clone())
            .hardware_config_flag(hardware_config_flag.clone())
            .audio_frame_cache_queue(audio_frame_cache_queue.clone())
            .video_frame_cache_queue(video_frame_cache_queue.clone())
            .audio_decode_thread_notify(audio_decode_thread_notify.clone())
            .video_decode_thread_notify(video_decode_thread_notify.clone())
            .current_video_timestamp(current_video_timestamp.clone())
            .demux_eof_flag(demux_eof_flag.clone())
            .build();
        let tiny_decoder = RwLock::new(TinyDecoder::new(tiny_decoder_creation_args)?);
        let realtime_status = RealtimeStatus {
            media_source_flag,
            audio_frame_recv: audio_frame_cache_queue.1,
            video_frame_recv: video_frame_cache_queue.1,
            demux_eof_flag,
            hardware_config_flag,
        };
        let background_tasks_notifies = BackgroundTasksNotifies {
            audio_decode_thread_notify,
            video_decode_thread_notify,
        };
        Ok(Arc::new(Self {
            tiny_decoder,
            media_source_info: RwLock::new(None),
            realtime_status,
            background_tasks_notifies,
        }))
    }
    /// Retrieve the source information, if not specified, returns Err.
    pub async fn media_source_info(&self) -> anyhow::Result<MediaSourceInfo> {
        let media_source_info = self.media_source_info.read().await;
        if let Some(info) = &*media_source_info {
            Ok(info.clone())
        } else {
            Err(anyhow::Error::msg("no source has been loaded"))
        }
    }
    /// Specify an input source.
    pub async fn reset_input(&self, path: &Path) -> anyhow::Result<()> {
        let mut decoder = self.tiny_decoder.write().await;
        if let Ok(info) = decoder.reset_input(path).await {
            *self.media_source_info.write().await = Some(info);
            Ok(())
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
    pub audio_frame_recv: Receiver<Audio>,
    pub video_frame_recv: Receiver<Video>,
    pub demux_eof_flag: Arc<AtomicBool>,
    pub hardware_config_flag: Arc<AtomicBool>,
}
/// Notifies for waking background tasks.
pub struct BackgroundTasksNotifies {
    pub audio_decode_thread_notify: Arc<Notify>,
    pub video_decode_thread_notify: Arc<Notify>,
}
/// Souce information, only exist after calling reset_input.
#[derive(Clone)]
pub struct MediaSourceInfo {
    pub transcoder_args: Option<TranscoderArgs>,
    pub stream_existence_flags: StreamExistenceFlags,
    pub end_timestamp: i64,
    pub end_time_formatted_string: String,
    pub cover_pic_data: Arc<RwLock<Option<Vec<u8>>>>,
    pub resolution_rect: [u32; 2],
    pub audio_time_base: Rational,
    pub video_time_base: Rational,
}
