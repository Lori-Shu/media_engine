//! The decode engine of tiny-player
use std::{
    path::Path,
    ptr::{null, null_mut},
    sync::{Arc, atomic::AtomicBool},
};

use anyhow::Context;

use crossfire::{
    AsyncRx, AsyncTx,
    spsc::{Array, bounded_async},
};
use ffmpeg_the_third::{
    ChannelLayout, Packet, Rational, Stream, codec,
    ffi::{
        AV_CHANNEL_LAYOUT_STEREO, AV_NOPTS_VALUE, AVCodecContext, AVHWDeviceType, AVPixelFormat,
        AVSEEK_FLAG_BACKWARD, AVSampleFormat, SwrContext, av_frame_copy_props,
        av_hwdevice_ctx_create, av_hwframe_transfer_data, avcodec_get_hw_config,
        swr_alloc_set_opts2, swr_convert_frame, swr_free, swr_init,
    },
    format::{Sample, sample::Type, stream::Disposition},
    frame::{Audio, Video},
};

use time::format_description;
use tokio::{
    runtime::Handle,
    sync::{Notify, RwLock},
    task::JoinHandle,
};
use tokio_util::{future::FutureExt, sync::CancellationToken};
use tracing::{Instrument, Level, info, span, warn};
use typed_builder::TypedBuilder;

use crate::{FrameReceivers, MediaSourceInfo, PlayerResult, StreamExistenceFlags, TranscoderArgs};
const AUDIO_SAMPLE_RATE: u32 = 48000;
/// this wrapper type should be protected manually to
/// keep memory safe in multi threads
/// means need to wrap an Arc and a Lock to use it in multi threads
struct ManualProtectedInput(ffmpeg_the_third::format::context::Input);
unsafe impl Sync for ManualProtectedInput {}
/// this wrapper type should be protected manually to
/// keep memory safe in multi threads
/// means need to wrap an Arc and a Lock to use it in multi threads
struct ManualProtectedVideoDecoder(ffmpeg_the_third::decoder::Video);
unsafe impl Sync for ManualProtectedVideoDecoder {}
/// this wrapper type should be protected manually to
/// keep memory safe in multi threads
/// means need to wrap an Arc and a Lock to use it in multi threads
struct ManualProtectedAudioDecoder(ffmpeg_the_third::decoder::Audio);
unsafe impl Sync for ManualProtectedAudioDecoder {}
/// this wrapper type should be protected manually to
/// keep memory safe in multi threads
/// means need to wrap an Arc and a Lock to use it in multi threads
struct ManualProtectedResampler(pub *mut SwrContext);
unsafe impl Send for ManualProtectedResampler {}
unsafe impl Sync for ManualProtectedResampler {}

struct ManualProtectedStream<'stream_life_time>(pub Stream<'stream_life_time>);
unsafe impl<'stream_life_time> Send for ManualProtectedStream<'stream_life_time> {}
unsafe impl<'stream_life_time> Sync for ManualProtectedStream<'stream_life_time> {}

/// `TinyDecoder` represent all the details and relevent member fields about
/// video format, decode, hardware acceleration etc.
/// the main struct of the decode_engine module to manage input and decode
pub(crate) struct TinyDecoder {
    video_stream_index: usize,
    audio_stream_index: usize,
    cover_stream_index: usize,
    video_time_base: Rational,
    audio_time_base: Rational,
    video_frame_rect: [u32; 2],
    format_duration: i64,
    format_input: Arc<RwLock<Option<ManualProtectedInput>>>,
    video_decoder: Arc<RwLock<Option<ManualProtectedVideoDecoder>>>,
    audio_decoder: Arc<RwLock<Option<ManualProtectedAudioDecoder>>>,
    resampler: Arc<RwLock<Option<ManualProtectedResampler>>>,
    demux_task_handle: Option<JoinHandle<PlayerResult<()>>>,
    video_decode_task_handle: Option<JoinHandle<PlayerResult<()>>>,
    audio_decode_task_handle: Option<JoinHandle<PlayerResult<()>>>,
    hardware_config_flag: Arc<AtomicBool>,
    cover_pic_data: Option<Box<[u8]>>,
    runtime_handle: Handle,
    demux_thread_notify: Arc<Notify>,
    media_source_flag: Arc<AtomicBool>,
    cancellation_token: Arc<CancellationToken>,
    demux_eof_flag: Arc<AtomicBool>,
    end_time_formatted_str: Box<str>,
    stream_existence_flags: StreamExistenceFlags,
    end_timestamp: i64,
    seek_flag: Arc<AtomicBool>,
}
impl TinyDecoder {
    /// init Decoder and new Struct
    pub fn new(tiny_decoder_creation_args: TinyDecoderArgs) -> PlayerResult<Self> {
        ffmpeg_the_third::init()?;
        let resampler = Arc::new(RwLock::new(None));
        let cancellation_token = Arc::new(CancellationToken::new());
        Ok(Self {
            video_stream_index: usize::MAX,
            audio_stream_index: usize::MAX,
            cover_stream_index: usize::MAX,
            stream_existence_flags: StreamExistenceFlags {
                video: false,
                audio: false,
            },
            video_time_base: Rational::new(1, 1),
            audio_time_base: Rational::new(1, 1),
            video_frame_rect: [0, 0],
            format_duration: 0,
            end_timestamp: 0,
            end_time_formatted_str: String::new().into_boxed_str(),
            format_input: Arc::new(RwLock::new(None)),
            video_decoder: Arc::new(RwLock::new(None)),
            audio_decoder: Arc::new(RwLock::new(None)),
            demux_task_handle: None,
            video_decode_task_handle: None,
            audio_decode_task_handle: None,
            hardware_config_flag: tiny_decoder_creation_args.hardware_config_flag,
            cover_pic_data: None,
            runtime_handle: tiny_decoder_creation_args.runtime_handle,
            demux_thread_notify: Arc::new(Notify::new()),
            resampler,
            media_source_flag: tiny_decoder_creation_args.media_source_flag,
            cancellation_token,
            demux_eof_flag: tiny_decoder_creation_args.demux_eof_flag,
            seek_flag: Arc::new(AtomicBool::new(false)),
        })
    }
    /// reset all fields to the initial state
    /// to prepare for fresh input
    async fn reset_states(&mut self) {
        self.audio_stream_index = usize::MAX;
        self.video_stream_index = usize::MAX;
        self.cover_stream_index = usize::MAX;
        self.stream_existence_flags = StreamExistenceFlags {
            audio: false,
            video: false,
        };
        *self.audio_decoder.write().await = None;

        self.audio_time_base = Rational::new(1, 1);
        self.free_swr_ctx();
        self.cover_pic_data = None;
        self.cancellation_token = Arc::new(CancellationToken::new());
        self.video_decode_task_handle = None;
        self.audio_decode_task_handle = None;
        self.demux_task_handle = None;
        self.end_time_formatted_str = String::new().into_boxed_str();
        self.end_timestamp = 0;
        self.format_duration = 0;
        *self.format_input.write().await = None;
        self.hardware_config_flag
            .store(false, std::sync::atomic::Ordering::Relaxed);
        *self.video_decoder.write().await = None;
        self.video_frame_rect = [0, 0];
        self.video_time_base = Rational::new(1, 1);
        self.media_source_flag
            .store(false, std::sync::atomic::Ordering::Relaxed);
        self.demux_eof_flag
            .store(false, std::sync::atomic::Ordering::Relaxed);
        self.seek_flag
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }
    /// `reset_input` is called when user selected a file path to play
    /// init all the details from the file selected
    pub async fn reset_input(
        &mut self,
        path: &Path,
    ) -> PlayerResult<(MediaSourceInfo, FrameReceivers)> {
        info!("ffmpeg version{}", ffmpeg_the_third::format::version());
        if self.demux_task_handle.is_some() {
            self.cancel_process_tasks().await?;
        }
        self.reset_states().await;
        let format_input = ManualProtectedInput(ffmpeg_the_third::format::input(path)?);
        info!("input construct finished");

        let found_streams = self.find_streams(&format_input).await;
        self.load_metadata(&format_input, &found_streams).await;
        if let Some(audio_stream) = &found_streams.audio_stream {
            let audio_decoder_ctx =
                ffmpeg_the_third::codec::Context::from_parameters(audio_stream.0.parameters())?;

            let mut audio_decoder = audio_decoder_ctx.decoder().audio()?;
            let audio_format = match audio_decoder.format() {
                Sample::None => AVSampleFormat::NONE,

                Sample::U8(t) => {
                    if t == Type::Packed {
                        AVSampleFormat::U8
                    } else {
                        AVSampleFormat::U8P
                    }
                }
                Sample::I16(t) => {
                    if t == Type::Packed {
                        AVSampleFormat::S16
                    } else {
                        AVSampleFormat::S16P
                    }
                }
                Sample::I32(t) => {
                    if t == Type::Packed {
                        AVSampleFormat::S32
                    } else {
                        AVSampleFormat::S32P
                    }
                }
                Sample::I64(t) => {
                    if t == Type::Packed {
                        AVSampleFormat::S64
                    } else {
                        AVSampleFormat::S64P
                    }
                }
                Sample::F32(t) => {
                    if t == Type::Packed {
                        AVSampleFormat::FLT
                    } else {
                        AVSampleFormat::FLTP
                    }
                }
                Sample::F64(t) => {
                    if t == Type::Packed {
                        AVSampleFormat::DBL
                    } else {
                        AVSampleFormat::DBLP
                    }
                }
            };
            if audio_decoder.ch_layout()
                == ChannelLayout::unspecified(audio_decoder.ch_layout().channels())
            {
                audio_decoder.set_ch_layout(ChannelLayout::default_for_channels(
                    audio_decoder.ch_layout().channels(),
                ));
            }
            let resampler = self.alloc_swr_ctx(&audio_decoder, audio_format)?;
            {
                let mut resampler_guard = self.resampler.write().await;
                *resampler_guard = Some(resampler);

                let mut a_decoder = self.audio_decoder.write().await;
                *a_decoder = Some(ManualProtectedAudioDecoder(audio_decoder));
            }
        }
        let transcoder_args = if let Some(video_stream) = &found_streams.video_stream {
            let codec_ctx =
                ffmpeg_the_third::codec::Context::from_parameters(video_stream.0.parameters())?;
            let video_decoder = self.enable_decoder_hwacc_with_fallback(codec_ctx).await?;
            let transcoder_args = TranscoderArgs {
                colorspace: video_decoder.color_space(),
                pixel_format: video_decoder.format(),
                transfer_characteristic: video_decoder.color_transfer_characteristic(),
                width: video_decoder.width(),
                height: video_decoder.height(),
            };
            info!("video decode format{:?}", video_decoder.format());
            self.video_frame_rect = [video_decoder.width(), video_decoder.height()];
            {
                let mut v_decoder = self.video_decoder.write().await;
                *v_decoder = Some(ManualProtectedVideoDecoder(video_decoder));
            }
            Some(transcoder_args)
        } else {
            None
        };

        {
            let mut input = self.format_input.write().await;
            *input = Some(format_input);
        }
        info!("initialzing parameters is finished!!!");
        let audio_frame_queue = bounded_async(8);
        let video_frame_queue = bounded_async(8);
        self.spawn_process_tasks(audio_frame_queue.0, video_frame_queue.0)
            .await;
        self.media_source_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok((
            MediaSourceInfo {
                transcoder_args,
                stream_existence_flags: self.stream_existence_flags.clone(),
                end_timestamp: self.end_timestamp,
                end_time_formatted_str: self.end_time_formatted_str.clone(),
                cover_pic_data: self.cover_pic_data.clone(),
                resolution_rect: self.video_frame_rect,
                audio_time_base: self.audio_time_base,
                video_time_base: self.video_time_base,
            },
            FrameReceivers {
                audio_frame_recv: audio_frame_queue.1,
                video_frame_recv: video_frame_queue.1,
            },
        ))
    }
    async fn find_streams<'a>(&mut self, input: &'a ManualProtectedInput) -> FoundStreams<'a> {
        let mut streams = FoundStreams {
            audio_stream: None,
            video_stream: None,
            cover_stream: None,
        };
        for item in input.0.streams() {
            let stream_type = item.parameters().medium();
            if item.disposition() == Disposition::ATTACHED_PIC {
                info!("pic stream was found");
                streams.cover_stream = Some(ManualProtectedStream(item));
            } else if stream_type == ffmpeg_the_third::util::media::Type::Video {
                info!("video stream was found");
                self.stream_existence_flags.video = true;
                streams.video_stream = Some(ManualProtectedStream(item));
            } else if stream_type == ffmpeg_the_third::util::media::Type::Audio {
                if streams.audio_stream.is_none() {
                    info!("audio stream was found");
                    self.stream_existence_flags.audio = true;
                    streams.audio_stream = Some(ManualProtectedStream(item));
                }
            } else if stream_type == ffmpeg_the_third::util::media::Type::Attachment {
                info!("attachment stream was found");
                streams.cover_stream = Some(ManualProtectedStream(item));
            }
        }
        streams
    }
    async fn load_metadata(
        &mut self,
        format_input: &ManualProtectedInput,
        found_streams: &FoundStreams<'_>,
    ) {
        if found_streams.audio_stream.is_none() && found_streams.video_stream.is_none() {
            warn!("no valid stream found");
        }
        if let Some(stream) = &found_streams.cover_stream {
            info!("cover stream found");
            self.cover_stream_index = stream.0.index();
            // SAFETY:
            // This unsafe block is promised to be safe because
            // the inner operations do not alloc extra memory
            // but only read and copy the memory of attached_pics
            unsafe {
                let avstream_ptr = stream.0.as_ptr();
                let data = (*avstream_ptr).attached_pic.data;
                let slice =
                    std::slice::from_raw_parts(data, (*avstream_ptr).attached_pic.size as usize);
                self.cover_pic_data = Some(slice.to_vec().into_boxed_slice());
            }
        }

        if let Some(stream) = &found_streams.audio_stream {
            self.audio_stream_index = stream.0.index();
            self.audio_time_base = stream.0.time_base();
            info!("audio time_base==={}", self.audio_time_base);
        }

        if let Some(stream) = &found_streams.video_stream {
            self.video_stream_index = stream.0.index();
            self.video_time_base = stream.0.time_base();
            info!("video time_base==={}", self.video_time_base);
        }

        // format_input.duration() can get the precise duration of the media file
        // format_input.duration() number unit is us
        info!("total duration {} us", format_input.0.duration());
        let format_duration = format_input.0.duration();
        if format_duration == AV_NOPTS_VALUE {
            self.format_duration = 0;
        } else {
            self.format_duration = format_duration;
        }

        let mut end_ts = AV_NOPTS_VALUE;
        if let Some(audio_stream) = found_streams.audio_stream.as_ref() {
            let audio_duration = audio_stream.0.duration();
            end_ts = audio_duration;
        }
        if end_ts == AV_NOPTS_VALUE
            && let Some(video_stream) = found_streams.video_stream.as_ref()
        {
            let video_duration = video_stream.0.duration();

            end_ts = video_duration;
        }
        if end_ts == AV_NOPTS_VALUE {
            if format_duration != AV_NOPTS_VALUE {
                info!("could not find duration in A/V streams, fallback to input duration");
                let mut temp_ts = i64::MAX;
                if found_streams.audio_stream.is_some() {
                    temp_ts = temp_ts.min(
                        format_duration * self.audio_time_base.denominator() as i64
                            / self.audio_time_base.numerator() as i64
                            / 1000000,
                    );
                }
                if found_streams.video_stream.is_some() {
                    temp_ts = temp_ts.min(
                        format_duration * self.video_time_base.denominator() as i64
                            / self.video_time_base.numerator() as i64
                            / 1000000,
                    );
                }
                end_ts = temp_ts;
            } else {
                end_ts = 0;
            }
        }
        self.end_timestamp = end_ts;
        self.load_end_time_str(end_ts).await;
    }

    /// start the demux and decode task
    async fn spawn_process_tasks(
        &mut self,
        audio_frame_sender: AsyncTx<Array<Audio>>,
        video_frame_sender: AsyncTx<Array<Video>>,
    ) {
        let audio_packet_cache_queue = bounded_async(32);
        let video_packet_cache_queue = bounded_async(32);

        let demux_context = DemuxContext::builder()
            .audio_stream_index(self.audio_stream_index)
            .video_stream_index(self.video_stream_index)
            .format_input(self.format_input.clone())
            .demux_thread_notify(self.demux_thread_notify.clone())
            .cancellation_token(self.cancellation_token.clone())
            .demux_eof_flag(self.demux_eof_flag.clone())
            .seek_flag(self.seek_flag.clone())
            .build();

        self.demux_task_handle = Some(self.runtime_handle.spawn(async move {
            let demux_span = span!(Level::INFO, "demux");
            let _demux_entered = demux_span.enter();
            demux_context
                .into_future(audio_packet_cache_queue.0, video_packet_cache_queue.0)
                .in_current_span()
                .await
        }));
        if self.stream_existence_flags.audio {
            let decode_context = AudioDecodeContext::builder()
                .audio_decoder(self.audio_decoder.clone())
                .resampler(self.resampler.clone())
                .cancellation_token(self.cancellation_token.clone())
                .build();

            self.audio_decode_task_handle = Some(self.runtime_handle.spawn(async move {
                let span = span!(Level::INFO, "decode");
                let _entered = span.enter();
                decode_context
                    .into_future(audio_packet_cache_queue.1, audio_frame_sender)
                    .in_current_span()
                    .await
            }));
        }
        if self.stream_existence_flags.video {
            let decode_context = VideoDecodeContext::builder()
                .video_decoder(self.video_decoder.clone())
                .hardware_config_flag(self.hardware_config_flag.clone())
                .cancellation_token(self.cancellation_token.clone())
                .build();

            self.video_decode_task_handle = Some(self.runtime_handle.spawn(async move {
                let span = span!(Level::INFO, "decode");
                let _entered = span.enter();
                decode_context
                    .into_future(video_packet_cache_queue.1, video_frame_sender)
                    .in_current_span()
                    .await
            }));
        }
    }

    /// seek the input to a selected timestamp
    pub async fn seek_timestamp_to_decode(&self, ts: i64) {
        let main_stream_idx = {
            if self.stream_existence_flags.audio {
                self.audio_stream_index
            } else {
                self.video_stream_index
            }
        };
        let mut input = self.format_input.write().await;
        // SAFETY:
        // This unsafe block is promised to be safe because
        // the inner operations do not alloc extra memory
        // but perform seek operation
        unsafe {
            info!("seek timestamp:{}", ts);
            if let Some(input) = &mut *input {
                let res = ffmpeg_the_third::ffi::avformat_seek_file(
                    input.0.as_mut_ptr(),
                    main_stream_idx as i32,
                    i64::MIN,
                    ts,
                    ts,
                    AVSEEK_FLAG_BACKWARD,
                );
                if res != 0 {
                    info!("seek err num:{res}");
                }
            }
        }
        self.seek_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);

        self.demux_eof_flag
            .store(false, std::sync::atomic::Ordering::Relaxed);
        self.demux_thread_notify.notify_one();
        info!("seek finished!");
    }
    /// stop demux and decode
    async fn cancel_process_tasks(&mut self) -> PlayerResult<()> {
        self.cancellation_token.cancel();

        if let Some(handle) = &mut self.demux_task_handle {
            // if handle.await.is_ok() {
            //     info!("demux thread join success");
            // }
            self.demux_thread_notify.notify_one();
            handle.await??;
            info!("demux thread joined");
        }
        if let Some(handle) = &mut self.audio_decode_task_handle {
            // if handle.await.is_ok() {
            //     info!("decode thread join success");
            // }
            handle.await??;
            info!("audio decode thread joined");
        }
        if let Some(handle) = &mut self.video_decode_task_handle {
            // if handle.await.is_ok() {
            //     info!("decode thread join success");
            // }
            handle.await??;
            info!("video decode thread joined");
        }

        Ok(())
    }
    /// use the file detail to compute the video duration and make str to inform the user
    async fn load_end_time_str(&mut self, end_ts: i64) {
        let sec_num = {
            if self.stream_existence_flags.audio {
                end_ts * self.audio_time_base.numerator() as i64
                    / self.audio_time_base.denominator() as i64
            } else {
                end_ts * self.video_time_base.numerator() as i64
                    / self.video_time_base.denominator() as i64
            }
        };
        let sec = (sec_num % 60) as u8;
        let min_num = sec_num / 60;
        let min = (min_num % 60) as u8;
        let hour_num = min_num / 60;
        let hour = hour_num as u8;
        info!("hour{},min{},sec{}", hour, min, sec);
        if let Ok(time) = time::Time::from_hms(hour, min, sec) {
            if let Ok(formatter) =
                format_description::parse_borrowed::<3>("[hour]:[minute]:[second]")
                && let Ok(s) = time.format(&formatter)
            {
                self.end_time_formatted_str = s.into_boxed_str();
            }
        } else {
            info!("end_time_err");
        }
    }
}

impl TinyDecoder {
    /// enable hardware accelerate for video decode
    /// fallback to softerware decoder if the local computer doesn't support
    async fn enable_decoder_hwacc_with_fallback(
        &mut self,
        codec_ctx: codec::Context,
    ) -> PlayerResult<ffmpeg_the_third::decoder::Video> {
        let mut decoder = codec_ctx.decoder().video()?;
        // SAFETY:
        // This unsafe block is promised to be safe because
        // the inner operations do not alloc extra memory
        // but perform enabling hardware acceleration
        unsafe {
            if let Some(codec) = &decoder.codec() {
                let mut idx = 0;

                let hw_config = loop {
                    let config = avcodec_get_hw_config(codec.as_ptr(), idx);

                    if config.is_null() {
                        break std::ptr::null();
                    }

                    if (*config).device_type == AVHWDeviceType::VULKAN {
                        break config;
                    }

                    idx += 1;
                };

                if hw_config.is_null() {
                    warn!("currently doesn't support hardware accelerate");
                    Ok(decoder)
                } else {
                    let mut hw_device_ctx = null_mut();
                    if 0 != av_hwdevice_ctx_create(
                        &mut hw_device_ctx,
                        (*hw_config).device_type,
                        null(),
                        null_mut(),
                        0,
                    ) {
                        warn!("hw device create err");
                        return Ok(decoder);
                    }
                    (*decoder.as_mut_ptr()).hw_device_ctx = hw_device_ctx;
                    (*decoder.as_mut_ptr()).get_format = Some(get_format_callback);
                    self.hardware_config_flag
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    warn!("hardware decode acceleration is on!");
                    Ok(decoder)
                }
            } else {
                Err(anyhow::Error::msg("err when config hardware acc"))
            }
        }
    }
    fn alloc_swr_ctx(
        &self,
        audio_decoder: &ffmpeg_the_third::decoder::Audio,
        audio_format: AVSampleFormat,
    ) -> PlayerResult<ManualProtectedResampler> {
        // SAFETY:
        // This unsafe block is promised to be safe because
        // the memory is bound to be freed when calling `reset_input` or
        // the TinyDecoder is dropped
        unsafe {
            let mut swr_ctx = null_mut();
            let r = swr_alloc_set_opts2(
                &mut swr_ctx,
                &AV_CHANNEL_LAYOUT_STEREO,
                ffmpeg_the_third::ffi::AVSampleFormat::FLT,
                AUDIO_SAMPLE_RATE as i32,
                audio_decoder.ch_layout().as_ptr(),
                audio_format,
                audio_decoder.rate() as i32,
                0,
                null_mut(),
            );
            if r < 0 {
                return Err(anyhow::Error::msg("swr ctx create err"));
            }
            let r = swr_init(swr_ctx);
            if r < 0 {
                return Err(anyhow::Error::msg("swr init err"));
            }
            Ok(ManualProtectedResampler(swr_ctx))
        }
    }
    fn free_swr_ctx(&self) {
        if let Ok(mut resampler) = self.resampler.try_write()
            && let Some(mut ctx) = resampler.take()
        {
            // SAFETY:
            // This unsafe block is promised to be safe because
            // the inner operations do not alloc extra memory
            // but free the allocated resampler context
            unsafe {
                swr_free(&mut ctx.0);
            }
        }
    }
}
unsafe extern "C" fn get_format_callback(
    _ctx: *mut AVCodecContext,
    fmt: *const AVPixelFormat,
) -> AVPixelFormat {
    // SAFETY:
    // This unsafe block is promised to be safe because
    // the inner operations do not alloc extra memory
    // but find the expected Vulkan pixel format
    unsafe {
        let mut i = 0;
        while *fmt.add(i) != AVPixelFormat::NONE {
            let current_fmt = *fmt.add(i);

            if current_fmt == AVPixelFormat::VULKAN {
                return current_fmt;
            }

            i += 1;
        }

        *fmt
    }
}
impl Drop for TinyDecoder {
    /// handle fields that have to be free manually
    fn drop(&mut self) {
        self.cancellation_token.cancel();
        self.demux_thread_notify.notify_waiters();
        self.free_swr_ctx();
    }
}
#[derive(TypedBuilder)]
pub(crate) struct TinyDecoderArgs {
    runtime_handle: Handle,
    media_source_flag: Arc<AtomicBool>,
    hardware_config_flag: Arc<AtomicBool>,
    demux_eof_flag: Arc<AtomicBool>,
}

#[derive(TypedBuilder)]
struct DemuxContext {
    audio_stream_index: usize,
    video_stream_index: usize,
    format_input: Arc<RwLock<Option<ManualProtectedInput>>>,
    demux_thread_notify: Arc<Notify>,
    cancellation_token: Arc<CancellationToken>,
    demux_eof_flag: Arc<AtomicBool>,
    seek_flag: Arc<AtomicBool>,
}
impl DemuxContext {
    /// the loop of demuxing video file
    async fn into_future(
        self,
        audio_packet_sender: AsyncTx<Array<PacketSignal>>,
        video_packet_sender: AsyncTx<Array<PacketSignal>>,
    ) -> PlayerResult<()> {
        info!("enter demux");
        while !self.cancellation_token.is_cancelled() {
            if self.seek_flag.load(std::sync::atomic::Ordering::Relaxed) {
                audio_packet_sender
                    .send(PacketSignal::Flush)
                    .with_cancellation_token(&self.cancellation_token)
                    .await;
                video_packet_sender
                    .send(PacketSignal::Flush)
                    .with_cancellation_token(&self.cancellation_token)
                    .await;
                self.seek_flag
                    .store(false, std::sync::atomic::Ordering::Relaxed);
            }
            let res = {
                let mut input = self.format_input.write().await;
                if let Some(input) = &mut *input {
                    match input.0.packets().next() {
                        Some(Ok((stream, packet))) => Ok((stream.index(), packet)),
                        None => Err(anyhow::Error::msg("EOF")),
                        _ => Err(anyhow::Error::msg("other cases")),
                    }
                } else {
                    Err(anyhow::Error::msg("other cases"))
                }
            };
            {
                let audio_stream_idx = self.audio_stream_index;
                let video_stream_idx = self.video_stream_index;

                match res {
                    Ok((stream_idx, packet)) => {
                        if stream_idx == audio_stream_idx {
                            if audio_packet_sender
                                .send(PacketSignal::Packet(packet))
                                .with_cancellation_token(&self.cancellation_token)
                                .await
                                .is_none()
                            {
                                return Ok(());
                            }
                        } else if stream_idx == video_stream_idx
                            && video_packet_sender
                                .send(PacketSignal::Packet(packet))
                                .with_cancellation_token(&self.cancellation_token)
                                .await
                                .is_none()
                        {
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        warn!("demux task detected err{}", e);
                        self.demux_eof_flag
                            .store(true, std::sync::atomic::Ordering::Relaxed);
                        self.demux_thread_notify.notified().await;
                    }
                }
            }
        }
        Ok(())
    }
}
#[derive(TypedBuilder)]
struct VideoDecodeContext {
    video_decoder: Arc<RwLock<Option<ManualProtectedVideoDecoder>>>,
    hardware_config_flag: Arc<AtomicBool>,
    cancellation_token: Arc<CancellationToken>,
}
impl VideoDecodeContext {
    /// the loop of decoding video packet
    async fn into_future(
        self,
        video_packet_recv: AsyncRx<Array<PacketSignal>>,
        video_frame_sender: AsyncTx<Array<Video>>,
    ) -> PlayerResult<()> {
        info!("enter decode");
        // let mut p = PathBuf::new();
        // let mut graph = Graph::new();
        // if decode_context.video_decoder.read().await.is_some() {
        //     if let Ok(exe_path) = CURRENT_EXE_PATH.as_ref() {
        //         if let Some(exe_folder) = exe_path.parent() {
        //             p = exe_folder.join("app_font.ttf");
        //             if tokio::fs::File::open(&p).await.is_err() {
        //                 if let Ok(mut file) = tokio::fs::File::create_new(&p).await {
        //                     if file.write_all(crate::appui::MAPLE_FONT).await.is_ok() {}
        //                 }
        //             }
        //         }
        //     }

        //     if let Some(font_path_str) = p.to_str() {
        //         let mut font_path_str = font_path_str.replace("\\", "/");
        //         if let Some(idx) = font_path_str.find(':') {
        //             font_path_str.insert(idx, '\\');
        //             unsafe {
        //                 if let Ok(c_str_buffer) = CString::new("buffer") {
        //                     if let Ok(c_str_buffersrc) = CString::new("buffersrc") {
        //                         if let Ok(c_str_buffersrc_args) = CString::new(format!(
        //                             "video_size={}x{}:pix_fmt={}:time_base={}/{}:pixel_aspect=1/1",
        //                             decode_context.video_frame_rect[0],
        //                             decode_context.video_frame_rect[1],
        //                             AVPixelFormat::AV_PIX_FMT_YUV420P as i32,
        //                             decode_context.video_time_base.numerator(),
        //                             decode_context.video_time_base.denominator(),
        //                         )) {
        //                             if let Ok(c_str_drawtext) = CString::new("drawtext") {
        //                                 if let Ok(c_str_draw) = CString::new("draw") {
        //                                     if let Ok(c_str_draw_args) = CString::new(format!(
        //                                         "text='Tiny Player':fontfile={}:fontsize=26:fontcolor=white@0.3:x=w-text_w-10:y=10",
        //                                         font_path_str
        //                                     )) {
        //                                         if let Ok(c_str_buffersink) =
        //                                             CString::new("buffersink")
        //                                         {
        //                                             if let Ok(c_str_sink) = CString::new("sink") {
        //                                                 let buffersrc_filter = avfilter_get_by_name(
        //                                                     c_str_buffer.as_ptr(),
        //                                                 );
        //                                                 // graph free will automatically free filterctx
        //                                                 let mut buffersrc_ctx = null_mut();
        //                                                 let draw_filter = avfilter_get_by_name(
        //                                                     c_str_drawtext.as_ptr(),
        //                                                 );
        //                                                 let mut drawtext_ctx = null_mut();
        //                                                 let buffersink_filter =
        //                                                     avfilter_get_by_name(
        //                                                         c_str_buffersink.as_ptr(),
        //                                                     );
        //                                                 let mut buffersink_ctx = null_mut();
        //                                                 let r = avfilter_graph_create_filter(
        //                                                     &mut buffersrc_ctx,
        //                                                     buffersrc_filter,
        //                                                     c_str_buffersrc.as_ptr(),
        //                                                     c_str_buffersrc_args.as_ptr(),
        //                                                     null_mut(),
        //                                                     graph.as_mut_ptr(),
        //                                                 );
        //                                                 if r < 0 {
        //                                                     info!("create buffer filter err");
        //                                                 }
        //                                                 let r = avfilter_graph_create_filter(
        //                                                     &mut drawtext_ctx,
        //                                                     draw_filter,
        //                                                     c_str_draw.as_ptr(),
        //                                                     c_str_draw_args.as_ptr(),
        //                                                     null_mut(),
        //                                                     graph.as_mut_ptr(),
        //                                                 );
        //                                                 if r < 0 {
        //                                                     info!("create drawtext filter err");
        //                                                 }
        //                                                 let r = avfilter_graph_create_filter(
        //                                                     &mut buffersink_ctx,
        //                                                     buffersink_filter,
        //                                                     c_str_sink.as_ptr(),
        //                                                     null(),
        //                                                     null_mut(),
        //                                                     graph.as_mut_ptr(),
        //                                                 );
        //                                                 if r < 0 {
        //                                                     info!("create buffersink filter err");
        //                                                 }
        //                                                 let r = avfilter_link(
        //                                                     buffersrc_ctx,
        //                                                     0,
        //                                                     drawtext_ctx,
        //                                                     0,
        //                                                 );
        //                                                 if r < 0 {
        //                                                     info!("link src and drawtext err");
        //                                                 }
        //                                                 let r = avfilter_link(
        //                                                     drawtext_ctx,
        //                                                     0,
        //                                                     buffersink_ctx,
        //                                                     0,
        //                                                 );
        //                                                 if r < 0 {
        //                                                     info!("link drawtext and sink err");
        //                                                 }
        //                                                 if graph.validate().is_ok() {
        //                                                     info!(
        //                                                         "graph validate success!dump:\n{}",
        //                                                         graph.dump()
        //                                                     );
        //                                                 }
        //                                             }
        //                                         }
        //                                     }
        //                                 }
        //                             }
        //                         }
        //                     }
        //                 }
        //             }
        //         }
        //     }
        // }
        // let hardware_frame_converter = Arc::new(RwLock::new(None));

        while !self.cancellation_token.is_cancelled() {
            if let Some(Ok(packet)) = video_packet_recv
                .recv()
                .with_cancellation_token(&self.cancellation_token)
                .await
            {
                match packet {
                    PacketSignal::Flush => {
                        let mut v_decoder = self.video_decoder.write().await;
                        if let Some(v) = &mut *v_decoder {
                            v.0.flush();
                        }
                    }
                    PacketSignal::Packet(packet) => {
                        let mut v_decoder = self.video_decoder.write().await;
                        // info!("video frame vec len{}", frames.len());
                        if let Some(decoder) = &mut *v_decoder
                            && decoder.0.send_packet(&packet).is_ok()
                        {
                            let mut video_frame_tmp = ffmpeg_the_third::frame::Video::empty();
                            while decoder.0.receive_frame(&mut video_frame_tmp).is_ok() {
                                if let Ok(video_frame) =
                                    self.transfer_hardware_frame(&video_frame_tmp).await
                                {
                                    video_frame_tmp = video_frame;
                                }

                                if video_frame_sender
                                    .send(video_frame_tmp.clone())
                                    .with_cancellation_token(&self.cancellation_token)
                                    .await
                                    .is_none()
                                {
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
            } else {
                return Ok(());
            }
        }
        Ok(())
    }
    /// transfer hardware frame data to standard frame data
    async fn transfer_hardware_frame(&self, video_frame_tmp: &Video) -> PlayerResult<Video> {
        if self
            .hardware_config_flag
            .load(std::sync::atomic::Ordering::Acquire)
        {
            // SAFETY:
            // This unsafe block is promised to be safe because
            // the inner operations do not alloc extra memory
            // but transfer hardware frame data to standard frame data
            unsafe {
                let mut transfered_frame = ffmpeg_the_third::frame::Video::empty();
                if 0 != av_hwframe_transfer_data(
                    transfered_frame.as_mut_ptr(),
                    video_frame_tmp.as_ptr(),
                    0,
                ) {
                    warn!("hardware frame transfer to software frame err");
                }
                av_frame_copy_props(transfered_frame.as_mut_ptr(), video_frame_tmp.as_ptr());
                return Ok(transfered_frame);
            }
        }
        Err(anyhow::Error::msg("hardware acceleration is not enabled"))
    }
}
#[derive(TypedBuilder)]
struct AudioDecodeContext {
    audio_decoder: Arc<RwLock<Option<ManualProtectedAudioDecoder>>>,
    resampler: Arc<RwLock<Option<ManualProtectedResampler>>>,
    cancellation_token: Arc<CancellationToken>,
}
impl AudioDecodeContext {
    /// the loop of decoding audio packet
    async fn into_future(
        self,
        audio_packet_recv: AsyncRx<Array<PacketSignal>>,
        audio_frame_sender: AsyncTx<Array<Audio>>,
    ) -> PlayerResult<()> {
        while !self.cancellation_token.is_cancelled() {
            if let Some(Ok(packet)) = audio_packet_recv
                .recv()
                .with_cancellation_token(&self.cancellation_token)
                .await
            {
                match packet {
                    PacketSignal::Flush => {
                        let mut a_decoder = self.audio_decoder.write().await;
                        if let Some(a) = &mut *a_decoder {
                            a.0.flush();
                        }
                    }
                    PacketSignal::Packet(packet) => {
                        let mut audio_decoder = self.audio_decoder.write().await;
                        if let Some(decoder) = &mut *audio_decoder
                            && decoder.0.send_packet(&packet).is_ok()
                        {
                            let mut resampler = self.resampler.write().await;
                            let resampler = resampler.as_mut().context("no resampler allocated")?;
                            let mut audio_frame_tmp = ffmpeg_the_third::frame::Audio::empty();
                            while decoder.0.receive_frame(&mut audio_frame_tmp).is_ok() {
                                let mut resampled_frame = Audio::empty();
                                resampled_frame.set_ch_layout(ChannelLayout::STEREO);
                                resampled_frame.set_rate(AUDIO_SAMPLE_RATE);
                                resampled_frame.set_format(Sample::F32(Type::Packed));
                                resampled_frame.set_pts(audio_frame_tmp.pts());
                                // SAFETY:
                                // This unsafe block is promised to be safe because
                                // the inner operations do not alloc extra memory
                                // but perform resampling
                                unsafe {
                                    if swr_convert_frame(
                                        resampler.0,
                                        resampled_frame.as_mut_ptr(),
                                        audio_frame_tmp.as_ptr(),
                                    ) != 0
                                    {
                                        warn!("convert audio frame err, but still in decoding!!!");
                                        return Err(anyhow::Error::msg("convert audio frame err"));
                                    }
                                }
                                if audio_frame_sender
                                    .send(resampled_frame)
                                    .with_cancellation_token(&self.cancellation_token)
                                    .await
                                    .is_none()
                                {
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
            } else {
                return Ok(());
            }
        }
        Ok(())
    }
}
struct FoundStreams<'a> {
    audio_stream: Option<ManualProtectedStream<'a>>,
    video_stream: Option<ManualProtectedStream<'a>>,
    cover_stream: Option<ManualProtectedStream<'a>>,
}
enum PacketSignal {
    Packet(Packet),
    Flush,
}
