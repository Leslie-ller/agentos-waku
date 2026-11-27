//! Full-pixel image validation, rich RPC error classification, and lossless
//! Antigravity trajectory DB media repair (AGE-173).
//!
//! When Antigravity reads a truncated/corrupt PNG (or other raster image) into
//! its native SQLite trajectory database (`<session_id>.db`), Google's backend
//! rejects every subsequent turn in that session with `400 INVALID_MEDIA`
//! (error 2011) or Claude's `Could not process image`. This module:
//!
//! 1. Validates inline image payloads with full framing + full scanline pixel
//!    decoding under bounded memory limits.
//! 2. Parses `google.rpc.ErrorInfo` / `LocalizedMessage` JSON details embedded
//!    in Antigravity `CortexErrorDetails` so users see the concrete `INVALID_MEDIA`
//!    reason instead of a generic connection failure.
//! 3. Safely backs up `<session_id>.db` via SQLite's online backup API (fail-closed)
//!    and losslessly strips only confirmed-corrupt inline image blobs from
//!    `steps.step_payload` using optimistic compare-and-swap updates, preserving
//!    all normal text, valid media, and unknown protobuf fields byte-for-byte.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OpenFlags};

const HARNESS_TOOL_RESPONSE_TYPE: &str = "antigravity.localharness.ToolResponse";

/// Cap per-image pixel buffer allocation during validation so malformed headers
/// (e.g., `65535x65535` decompression bombs) fail deterministically without OOM.
const MAX_IMAGE_DECODE_ALLOC_BYTES: u64 = 256 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Rich Google RPC error extraction (`google.rpc.ErrorInfo` / `LocalizedMessage`)
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct RpcErrorInfo {
    pub reason: Option<String>,
    pub domain: Option<String>,
    pub error_number: Option<String>,
    pub localized_message: Option<String>,
}

impl RpcErrorInfo {
    pub(crate) fn is_empty(&self) -> bool {
        self.reason.is_none()
            && self.domain.is_none()
            && self.error_number.is_none()
            && self.localized_message.is_none()
    }

    pub(crate) fn is_invalid_media(&self) -> bool {
        self.reason
            .as_deref()
            .is_some_and(|r| r.trim().eq_ignore_ascii_case("INVALID_MEDIA"))
            || self
                .error_number
                .as_deref()
                .is_some_and(|n| n.trim() == "2011")
    }

    fn merge_json_detail(&mut self, obj: &serde_json::Map<String, serde_json::Value>) {
        let type_url = obj
            .get("@type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if self.reason.is_none()
            && let Some(reason) = obj
                .get("reason")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
        {
            self.reason = Some(reason.to_string());
        }
        if self.domain.is_none()
            && let Some(domain) = obj
                .get("domain")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
        {
            self.domain = Some(domain.to_string());
        }
        if self.error_number.is_none()
            && let Some(num) = obj
                .get("metadata")
                .and_then(serde_json::Value::as_object)
                .and_then(|m| m.get("error_number"))
                .and_then(|v| {
                    v.as_str()
                        .map(str::to_string)
                        .or_else(|| v.as_i64().map(|n| n.to_string()))
                        .or_else(|| v.as_u64().map(|n| n.to_string()))
                })
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        {
            self.error_number = Some(num);
        }
        if self.localized_message.is_none()
            && type_url
                .rsplit('/')
                .next()
                .is_some_and(|t| t == "LocalizedMessage")
            && let Some(msg) = obj
                .get("message")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
        {
            self.localized_message = Some(msg.to_string());
        }
    }

    fn merge_json_value(&mut self, value: &serde_json::Value) {
        let Some(obj) = value.as_object() else {
            if let Some(arr) = value.as_array() {
                for item in arr {
                    self.merge_json_value(item);
                }
            }
            return;
        };
        self.merge_json_detail(obj);
        if let Some(details) = obj.get("details").and_then(serde_json::Value::as_array) {
            for item in details {
                self.merge_json_value(item);
            }
        }
        if let Some(err) = obj.get("error") {
            self.merge_json_value(err);
        }
    }

    pub(crate) fn prefix_badge(&self) -> Option<String> {
        match (self.reason.as_deref(), self.error_number.as_deref()) {
            (Some(reason), Some(num)) => Some(format!("{reason} (error {num})")),
            (Some(reason), None) => Some(reason.to_string()),
            (None, Some(num)) => Some(format!("error {num}")),
            (None, None) => None,
        }
    }
}

pub(crate) fn extract_rpc_error_info(
    rpc_error_details: &[String],
    details: &str,
    full_error: &str,
    user_error_number: &str,
) -> Option<RpcErrorInfo> {
    let mut info = RpcErrorInfo::default();
    for raw in rpc_error_details {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed) {
            info.merge_json_value(&val);
        }
    }
    if !details.trim().is_empty()
        && let Ok(val) = serde_json::from_str::<serde_json::Value>(details.trim())
    {
        info.merge_json_value(&val);
    }
    if info.reason.is_none()
        && !full_error.trim().is_empty()
        && let Some(json_start) = full_error.find('{')
        && let Ok(val) =
            serde_json::from_str::<serde_json::Value>(full_error[json_start..].trim())
    {
        info.merge_json_value(&val);
    }
    let user_num = user_error_number.trim();
    if info.error_number.is_none() && !user_num.is_empty() {
        info.error_number = Some(user_num.to_string());
    }
    (!info.is_empty()).then_some(info)
}

pub(crate) fn decorate_with_rpc_info(base: &str, rpc_info: Option<&RpcErrorInfo>) -> String {
    let trimmed = base.trim();
    let Some(info) = rpc_info else {
        return trimmed.to_string();
    };
    let detail = info
        .localized_message
        .as_deref()
        .filter(|m| !m.is_empty() && !trimmed.contains(m))
        .map(|m| {
            if trimmed.is_empty() {
                m.to_string()
            } else {
                format!("{m} ({trimmed})")
            }
        })
        .unwrap_or_else(|| trimmed.to_string());
    let Some(badge) = info.prefix_badge() else {
        return detail;
    };
    if let Some(reason) = info.reason.as_deref()
        && detail
            .to_ascii_lowercase()
            .contains(&reason.to_ascii_lowercase())
    {
        if let Some(num) = info.error_number.as_deref()
            && !detail.contains(num)
        {
            return format!("{detail} (error {num})");
        }
        return detail;
    }
    if detail.is_empty() {
        badge
    } else {
        format!("{badge}: {detail}")
    }
}

pub(crate) fn is_invalid_media_error_text(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("invalid_media")
        || lower.contains("unable to process input image")
        || lower.contains("could not process image")
        || lower.contains("image dimensions exceed")
        || lower.contains("image exceeds")
}

// ---------------------------------------------------------------------------
// Image validation & lossless trajectory media sanitizer
// ---------------------------------------------------------------------------

/// Only confirmed corruption authorizes removal from persisted history.
#[derive(Debug)]
enum ImageMediaError {
    Corrupt(String),
    Unsupported(String),
    ResourceLimit(String),
    Unavailable(String),
}

impl ImageMediaError {
    fn into_reason(self) -> String {
        match self {
            Self::Corrupt(reason)
            | Self::Unsupported(reason)
            | Self::ResourceLimit(reason)
            | Self::Unavailable(reason) => reason,
        }
    }

    fn corruption_reason(self) -> Option<String> {
        match self {
            Self::Corrupt(reason) => Some(reason),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemovedMediaItem {
    pub step_idx: i64,
    pub mime_type: String,
    pub byte_len: usize,
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct MediaSanitizeReport {
    pub backup_path: Option<PathBuf>,
    pub removed: Vec<RemovedMediaItem>,
}

impl MediaSanitizeReport {
    pub(crate) fn is_modified(&self) -> bool {
        !self.removed.is_empty()
    }
}

pub(crate) fn format_media_repair_summary(report: &MediaSanitizeReport) -> Option<(String, String)> {
    if !report.is_modified() {
        return None;
    }
    let count = report.removed.len();
    let backup_name = report
        .backup_path
        .as_ref()
        .and_then(|p| p.file_name())
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "trajectory backup".into());
    let first = &report.removed[0];
    let title = if count == 1 {
        format!(
            "Removed 1 unreadable image ({}) from Antigravity session history",
            first.mime_type
        )
    } else {
        format!("Removed {count} unreadable images from Antigravity session history")
    };
    let detail = format!(
        "Original trajectory DB backed up as {backup_name}. Cause: {}",
        first.reason
    );
    Some((title, detail))
}

fn crc32_ieee(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in bytes {
        crc ^= u32::from(b);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Verify complete PNG chunk framing (`IHDR` .. `IEND`), non-empty `IDAT`, and
/// every chunk's IEEE CRC-32 before running full scanline decompression.
fn validate_png_framing(data: &[u8]) -> Result<(), String> {
    const PNG_SIG: &[u8; 8] = b"\x89PNG\r\n\x1a\n";
    if !data.starts_with(PNG_SIG) {
        if let Some(offset) = data.windows(PNG_SIG.len()).position(|w| w == PNG_SIG) {
            return Err(format!(
                "non-PNG prefix ({offset} leading bytes before PNG signature)"
            ));
        }
        return Err("missing PNG signature".into());
    }
    let mut pos = PNG_SIG.len();
    let mut saw_ihdr = false;
    let mut saw_idat = false;
    let mut saw_iend = false;
    while pos < data.len() {
        let header_end = pos
            .checked_add(8)
            .filter(|&end| end <= data.len())
            .ok_or_else(|| "truncated PNG chunk header".to_string())?;
        let chunk_len = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let chunk_type: [u8; 4] = data[pos + 4..header_end].try_into().unwrap();
        let data_end = header_end
            .checked_add(chunk_len)
            .ok_or_else(|| "PNG chunk length overflow".to_string())?;
        let crc_end = data_end
            .checked_add(4)
            .filter(|&end| end <= data.len())
            .ok_or_else(|| {
                format!(
                    "truncated PNG {} chunk (expected {} bytes, file ended early)",
                    String::from_utf8_lossy(&chunk_type),
                    chunk_len
                )
            })?;
        let expected_crc = u32::from_be_bytes(data[data_end..crc_end].try_into().unwrap());
        let actual_crc = crc32_ieee(&data[pos + 4..data_end]);
        if actual_crc != expected_crc {
            return Err(format!(
                "corrupt PNG {} chunk CRC (expected {expected_crc:#010x}, got {actual_crc:#010x})",
                String::from_utf8_lossy(&chunk_type)
            ));
        }
        if !saw_ihdr {
            if &chunk_type != b"IHDR" || chunk_len != 13 {
                return Err("PNG missing valid leading IHDR chunk".into());
            }
            saw_ihdr = true;
        } else if &chunk_type == b"IDAT" && chunk_len > 0 {
            saw_idat = true;
        } else if &chunk_type == b"IEND" {
            if chunk_len != 0 {
                return Err("non-empty PNG IEND chunk".into());
            }
            saw_iend = true;
            pos = crc_end;
            break;
        }
        pos = crc_end;
    }
    if !saw_ihdr {
        return Err("PNG missing IHDR chunk".into());
    }
    if !saw_idat {
        return Err("PNG missing IDAT image data".into());
    }
    if !saw_iend {
        return Err("truncated PNG: missing IEND chunk".into());
    }
    if pos != data.len() {
        return Err(format!(
            "trailing {} bytes after PNG IEND chunk",
            data.len() - pos
        ));
    }
    Ok(())
}

/// Verify JPEG starts with SOI (`FF D8 FF`) and contains a terminating EOI
/// (`FF D9`) marker.
fn validate_jpeg_framing(data: &[u8]) -> Result<(), String> {
    if data.len() < 4 || !data.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return Err("missing JPEG SOI header".into());
    }
    if !data[2..].windows(2).any(|w| w == [0xFF, 0xD9]) {
        return Err("truncated JPEG: missing EOI marker".into());
    }
    Ok(())
}

/// Full-pixel decode validation for inline image media (`image/png`,
/// `image/jpeg`, `image/webp`, `image/gif`, or sniffed raster bytes).
#[cfg(test)]
pub(crate) fn validate_image_media_bytes(mime_type: &str, data: &[u8]) -> Result<(), String> {
    check_image_media_bytes(mime_type, data).map_err(ImageMediaError::into_reason)
}

fn check_image_media_bytes(mime_type: &str, data: &[u8]) -> Result<(), ImageMediaError> {
    let mime = mime_type.trim().to_ascii_lowercase();
    let claims_image = mime.starts_with("image/");
    if data.is_empty() {
        return if claims_image {
            Err(ImageMediaError::Corrupt("empty image payload".into()))
        } else {
            Ok(())
        };
    }

    let declared_format = match mime.as_str() {
        "image/png" => Some(image::ImageFormat::Png),
        "image/jpeg" | "image/jpg" => Some(image::ImageFormat::Jpeg),
        "image/webp" => Some(image::ImageFormat::WebP),
        "image/gif" => Some(image::ImageFormat::Gif),
        _ => None,
    };

    let format = match declared_format {
        Some(fmt) => Some(fmt),
        None => match image::guess_format(data) {
            Ok(
                fmt @ (image::ImageFormat::Png
                | image::ImageFormat::Jpeg
                | image::ImageFormat::WebP
                | image::ImageFormat::Gif),
            ) => Some(fmt),
            _ if claims_image => {
                if let Some(offset) = data
                    .windows(8)
                    .position(|w| w == b"\x89PNG\r\n\x1a\n")
                    .filter(|&o| o > 0)
                {
                    return Err(ImageMediaError::Corrupt(format!(
                        "non-PNG prefix ({offset} leading bytes before PNG signature)"
                    )));
                }
                None
            }
            _ => None,
        },
    };

    let Some(format) = format else {
        return if claims_image {
            Err(ImageMediaError::Unsupported(format!(
                "unsupported or unrecognizable image format ({mime})"
            )))
        } else {
            Ok(())
        };
    };

    match format {
        image::ImageFormat::Png => validate_png_framing(data).map_err(ImageMediaError::Corrupt)?,
        image::ImageFormat::Jpeg => {
            validate_jpeg_framing(data).map_err(ImageMediaError::Corrupt)?
        }
        _ => {}
    }

    let mut reader = image::ImageReader::with_format(std::io::Cursor::new(data), format);
    let mut limits = image::Limits::default();
    limits.max_alloc = Some(MAX_IMAGE_DECODE_ALLOC_BYTES);
    reader.limits(limits);
    let decoded = reader.decode().map_err(|err| {
        let reason = format!("full image decode failed: {err}");
        match err {
            image::ImageError::Limits(_) => ImageMediaError::ResourceLimit(reason),
            image::ImageError::Unsupported(_) => ImageMediaError::Unsupported(reason),
            image::ImageError::Decoding(_) => ImageMediaError::Corrupt(reason),
            image::ImageError::IoError(ref err)
                if err.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                ImageMediaError::Corrupt(reason)
            }
            _ => ImageMediaError::Unavailable(reason),
        }
    })?;
    if decoded.width() == 0 || decoded.height() == 0 {
        return Err(ImageMediaError::Corrupt(
            "image has zero width or height".into(),
        ));
    }
    Ok(())
}

/// Decode base64 (tolerating whitespace / URL-safe alphabet) and run full pixel
/// validation.
#[allow(dead_code)]
pub(crate) fn validate_base64_image_media(mime_type: &str, b64: &str) -> Result<(), String> {
    check_base64_image_media(mime_type, b64).map_err(ImageMediaError::into_reason)
}

fn check_base64_image_media(mime_type: &str, b64: &str) -> Result<(), ImageMediaError> {
    use base64::Engine as _;
    let compact: String = b64.chars().filter(|c| !c.is_ascii_whitespace()).collect();
    if compact.is_empty() {
        return Err(ImageMediaError::Corrupt(
            "empty base64 image payload".into(),
        ));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&compact)
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(&compact))
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(&compact))
        .or_else(|_| base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(&compact))
        .map_err(|err| ImageMediaError::Corrupt(format!("invalid base64 image encoding: {err}")))?;
    let effective_mime = if mime_type.trim().is_empty() {
        "image/png"
    } else {
        mime_type
    };
    check_image_media_bytes(effective_mime, &bytes)
}

#[derive(Clone, Copy)]
enum WireValue<'a> {
    Bytes(&'a [u8]),
    Other,
}

#[derive(Clone, Copy)]
struct WireField<'a> {
    field_number: u32,
    value: WireValue<'a>,
    raw: &'a [u8],
}

fn read_varint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    while shift < 64 {
        let b = *buf.get(*pos)?;
        *pos += 1;
        if shift == 63 && b > 1 {
            return None;
        }
        result |= u64::from(b & 0x7F) << shift;
        if b & 0x80 == 0 {
            return Some(result);
        }
        shift += 7;
    }
    None
}

fn encode_varint(mut val: u64, out: &mut Vec<u8>) {
    while val >= 0x80 {
        out.push((val as u8) | 0x80);
        val >>= 7;
    }
    out.push(val as u8);
}

fn encode_len_delimited(field_number: u32, payload: &[u8], out: &mut Vec<u8>) {
    encode_varint((u64::from(field_number) << 3) | 2, out);
    encode_varint(payload.len() as u64, out);
    out.extend_from_slice(payload);
}

fn parse_wire_fields(buf: &[u8]) -> Option<Vec<WireField<'_>>> {
    let mut pos = 0usize;
    let mut fields = Vec::new();
    while pos < buf.len() {
        let raw_start = pos;
        let key = read_varint(buf, &mut pos)?;
        let field_number = u32::try_from(key >> 3).ok().filter(|&n| n > 0)?;
        let wire_type = (key & 0x07) as u8;
        let value = match wire_type {
            0 => {
                read_varint(buf, &mut pos)?;
                WireValue::Other
            }
            1 => {
                pos = pos.checked_add(8).filter(|&end| end <= buf.len())?;
                WireValue::Other
            }
            2 => {
                let len = usize::try_from(read_varint(buf, &mut pos)?).ok()?;
                let end = pos.checked_add(len).filter(|&end| end <= buf.len())?;
                let slice = &buf[pos..end];
                pos = end;
                WireValue::Bytes(slice)
            }
            5 => {
                pos = pos.checked_add(4).filter(|&end| end <= buf.len())?;
                WireValue::Other
            }
            _ => return None,
        };
        fields.push(WireField {
            field_number,
            value,
            raw: &buf[raw_start..pos],
        });
    }
    Some(fields)
}

#[derive(Debug)]
struct CorruptMediaDiag {
    mime_type: String,
    byte_len: usize,
    reason: String,
}

impl CorruptMediaDiag {
    fn format_note(&self) -> String {
        let mime = if self.mime_type.is_empty() {
            "image/*"
        } else {
            self.mime_type.as_str()
        };
        format!(
            "[agentos: removed unreadable image media ({mime}, {} bytes: {}) after backing up trajectory DB]",
            self.byte_len, self.reason
        )
    }
}

/// Inspect `.exa.codeium_common_pb.Media` wire bytes (`1: mime_type`,
/// `2: inline_data`, `3: blobref`). Returns `Some(diag)` ONLY when the entry
/// carries corrupt or empty inline image data without a blobref.
fn check_media_proto_wire(buf: &[u8]) -> Option<CorruptMediaDiag> {
    let fields = parse_wire_fields(buf)?;
    let mut mime_type = String::new();
    let mut inline_data: &[u8] = &[];
    let mut has_blobref = false;
    for f in fields {
        if let WireValue::Bytes(bytes) = f.value {
            match f.field_number {
                1 => mime_type = String::from_utf8_lossy(bytes).into_owned(),
                2 => inline_data = bytes,
                3 => has_blobref = true,
                _ => {}
            }
        }
    }
    let normalized_mime = mime_type.trim().to_ascii_lowercase();
    let looks_like_image = normalized_mime.starts_with("image/")
        || (normalized_mime.is_empty()
            && !inline_data.is_empty()
            && matches!(
                image::guess_format(inline_data),
                Ok(image::ImageFormat::Png
                    | image::ImageFormat::Jpeg
                    | image::ImageFormat::WebP
                    | image::ImageFormat::Gif)
            ));
    if !looks_like_image {
        return None;
    }
    if inline_data.is_empty() {
        if has_blobref {
            return None;
        }
        return Some(CorruptMediaDiag {
            mime_type: normalized_mime,
            byte_len: 0,
            reason: "empty image payload".into(),
        });
    }
    let check_mime = if normalized_mime.is_empty() {
        "image/*"
    } else {
        normalized_mime.as_str()
    };
    check_image_media_bytes(check_mime, inline_data)
        .err()
        .and_then(ImageMediaError::corruption_reason)
        .map(|reason| CorruptMediaDiag {
            mime_type: normalized_mime,
            byte_len: inline_data.len(),
            reason,
        })
}

/// Inspect `.exa.codeium_common_pb.ImageData` wire bytes (`1: base64_data`,
/// `2: mime_type`).
fn check_image_data_proto_wire(buf: &[u8]) -> Option<CorruptMediaDiag> {
    let fields = parse_wire_fields(buf)?;
    let mut base64_data = String::new();
    let mut mime_type = String::new();
    for f in fields {
        if let WireValue::Bytes(bytes) = f.value {
            match f.field_number {
                1 => base64_data = String::from_utf8_lossy(bytes).into_owned(),
                2 => mime_type = String::from_utf8_lossy(bytes).into_owned(),
                _ => {}
            }
        }
    }
    let normalized_mime = mime_type.trim().to_ascii_lowercase();
    if !normalized_mime.is_empty() && !normalized_mime.starts_with("image/") {
        return None;
    }
    check_base64_image_media(&normalized_mime, &base64_data)
        .err()
        .and_then(ImageMediaError::corruption_reason)
        .map(|reason| CorruptMediaDiag {
            mime_type: if normalized_mime.is_empty() {
                "image/*".into()
            } else {
                normalized_mime
            },
            byte_len: base64_data.len(),
            reason,
        })
}

#[derive(Clone, Copy)]
enum TextNotePlacement {
    None,
    SingularStringField(u32),
    UserInputQueryOrItems,
}

fn append_note_text(existing: &str, notes: &[String]) -> String {
    let suffix = notes.join("\n");
    if existing.trim().is_empty() {
        suffix
    } else {
        format!("{existing}\n{suffix}")
    }
}

/// Losslessly rewrite a container message that may hold `Media` (`media_tags`)
/// and/or `ImageData` (`image_data_tags`) fields. Returns `None` if no corrupt
/// media was found (keeping the caller's bytes 100% untouched).
fn rewrite_media_container(
    buf: &[u8],
    media_tags: &[u32],
    image_data_tags: &[u32],
    note_placement: TextNotePlacement,
    removed_out: &mut Vec<CorruptMediaDiag>,
) -> Option<Vec<u8>> {
    let fields = parse_wire_fields(buf)?;
    let mut local_removed: Vec<CorruptMediaDiag> = Vec::new();
    let mut keep_flags = Vec::with_capacity(fields.len());

    for f in &fields {
        let diag = match f.value {
            WireValue::Bytes(bytes) if media_tags.contains(&f.field_number) => {
                check_media_proto_wire(bytes)
            }
            WireValue::Bytes(bytes) if image_data_tags.contains(&f.field_number) => {
                check_image_data_proto_wire(bytes)
            }
            _ => None,
        };
        if let Some(d) = diag {
            local_removed.push(d);
            keep_flags.push(false);
        } else {
            keep_flags.push(true);
        }
    }

    if local_removed.is_empty() {
        return None;
    }

    let notes: Vec<String> = local_removed
        .iter()
        .map(CorruptMediaDiag::format_note)
        .collect();
    removed_out.extend(local_removed);

    let mut out = Vec::with_capacity(buf.len());
    match note_placement {
        TextNotePlacement::None => {
            for (f, &keep) in fields.iter().zip(keep_flags.iter()) {
                if keep {
                    out.extend_from_slice(f.raw);
                }
            }
        }
        TextNotePlacement::SingularStringField(text_tag) => {
            let mut existing_text = String::new();
            for (f, &keep) in fields.iter().zip(keep_flags.iter()) {
                if keep
                    && f.field_number == text_tag
                    && let WireValue::Bytes(bytes) = f.value
                {
                    existing_text = String::from_utf8_lossy(bytes).into_owned();
                }
            }
            let combined = append_note_text(&existing_text, &notes);
            encode_len_delimited(text_tag, combined.as_bytes(), &mut out);
            for (f, &keep) in fields.iter().zip(keep_flags.iter()) {
                if keep && !(f.field_number == text_tag && matches!(f.value, WireValue::Bytes(_))) {
                    out.extend_from_slice(f.raw);
                }
            }
        }
        TextNotePlacement::UserInputQueryOrItems => {
            let mut query_text = String::new();
            let mut has_items = false;
            for (f, &keep) in fields.iter().zip(keep_flags.iter()) {
                if !keep {
                    continue;
                }
                if f.field_number == 1 {
                    if let WireValue::Bytes(bytes) = f.value {
                        query_text = String::from_utf8_lossy(bytes).into_owned();
                    }
                } else if f.field_number == 3 {
                    has_items = true;
                }
            }
            if query_text.trim().is_empty() && has_items {
                for (f, &keep) in fields.iter().zip(keep_flags.iter()) {
                    if keep {
                        out.extend_from_slice(f.raw);
                    }
                }
                let item_note = format!("\n{}", notes.join("\n"));
                let mut item_buf = Vec::with_capacity(item_note.len() + 4);
                encode_len_delimited(1, item_note.as_bytes(), &mut item_buf);
                encode_len_delimited(3, &item_buf, &mut out);
            } else {
                let combined = append_note_text(&query_text, &notes);
                encode_len_delimited(1, combined.as_bytes(), &mut out);
                for (f, &keep) in fields.iter().zip(keep_flags.iter()) {
                    if keep && f.field_number != 1 {
                        out.extend_from_slice(f.raw);
                    }
                }
            }
        }
    }

    Some(out)
}

/// Losslessly rewrite `CortexStepAgencyToolCall` (`Step` tag 116), whose
/// `response_messages` (`4: repeated google.protobuf.Any`) wraps
/// `antigravity.localharness.ToolResponse` (`2: response_json`,
/// `3: repeated supplemental_media`).
fn rewrite_agency_tool_call_step(
    buf: &[u8],
    removed_out: &mut Vec<CorruptMediaDiag>,
) -> Option<Vec<u8>> {
    let fields = parse_wire_fields(buf)?;
    let mut changed = false;
    let mut out = Vec::with_capacity(buf.len());
    for f in fields {
        if f.field_number == 4
            && let WireValue::Bytes(any_bytes) = f.value
            && let Some(any_fields) = parse_wire_fields(any_bytes)
        {
            let mut type_url = String::new();
            for af in &any_fields {
                if af.field_number == 1
                    && let WireValue::Bytes(tb) = af.value
                {
                    type_url = String::from_utf8_lossy(tb).into_owned();
                }
            }
            let is_tool_resp = type_url
                .rsplit('/')
                .next()
                .is_some_and(|name| name == HARNESS_TOOL_RESPONSE_TYPE);
            if is_tool_resp {
                let mut any_changed = false;
                let mut new_any = Vec::with_capacity(any_bytes.len());
                for af in any_fields {
                    if af.field_number == 2
                        && let WireValue::Bytes(resp_bytes) = af.value
                        && let Some(rewritten_resp) = rewrite_media_container(
                            resp_bytes,
                            &[3],
                            &[],
                            TextNotePlacement::None,
                            removed_out,
                        )
                    {
                        encode_len_delimited(2, &rewritten_resp, &mut new_any);
                        any_changed = true;
                        continue;
                    }
                    new_any.extend_from_slice(af.raw);
                }
                if any_changed {
                    encode_len_delimited(4, &new_any, &mut out);
                    changed = true;
                    continue;
                }
            }
        }
        out.extend_from_slice(f.raw);
    }
    changed.then_some(out)
}

/// Losslessly rewrite `CortexStepGeneric` (`Step` tag 140), whose `result`
/// (`GenericStepResult` at tag 2) carries `1: result` and `4: repeated media`.
fn rewrite_generic_step(buf: &[u8], removed_out: &mut Vec<CorruptMediaDiag>) -> Option<Vec<u8>> {
    let fields = parse_wire_fields(buf)?;
    let mut changed = false;
    let mut out = Vec::with_capacity(buf.len());
    for f in fields {
        if f.field_number == 2
            && let WireValue::Bytes(result_bytes) = f.value
            && let Some(rewritten_result) = rewrite_media_container(
                result_bytes,
                &[4],
                &[],
                TextNotePlacement::SingularStringField(1),
                removed_out,
            )
        {
            encode_len_delimited(2, &rewritten_result, &mut out);
            changed = true;
            continue;
        }
        out.extend_from_slice(f.raw);
    }
    changed.then_some(out)
}

/// Losslessly strip corrupt `Media` / `ImageData` payloads from a serialized
/// `gemini_coder.Step`, preserving every valid media item, normal message text,
/// and unknown protobuf field byte-for-byte.
fn rewrite_step_corrupt_media(step_payload: &[u8]) -> Option<(Vec<u8>, Vec<CorruptMediaDiag>)> {
    let fields = parse_wire_fields(step_payload)?;
    let mut removed = Vec::new();
    let mut changed = false;
    let mut out = Vec::with_capacity(step_payload.len());

    for f in fields {
        if let WireValue::Bytes(arm_bytes) = f.value {
            let rewritten_arm = match f.field_number {
                // `14: view_file` (`4: content`, `14: binary_data`, `15: media_data`)
                14 => rewrite_media_container(
                    arm_bytes,
                    &[15],
                    &[14],
                    TextNotePlacement::SingularStringField(4),
                    &mut removed,
                ),
                // `19: user_input` (`1: query`, `3: items`, `5: images`, `9: media`)
                19 => rewrite_media_container(
                    arm_bytes,
                    &[9],
                    &[5],
                    TextNotePlacement::UserInputQueryOrItems,
                    &mut removed,
                ),
                // `20: planner_response` (`1: response`, `20: media`)
                20 => rewrite_media_container(
                    arm_bytes,
                    &[20],
                    &[],
                    TextNotePlacement::SingularStringField(1),
                    &mut removed,
                ),
                // `47: mcp_tool` (`3: result_string`, `5: images`, `6: media`)
                47 => rewrite_media_container(
                    arm_bytes,
                    &[6],
                    &[5],
                    TextNotePlacement::SingularStringField(3),
                    &mut removed,
                ),
                // `114: system_message` (`1: message`, `6: media`)
                114 => rewrite_media_container(
                    arm_bytes,
                    &[6],
                    &[],
                    TextNotePlacement::SingularStringField(1),
                    &mut removed,
                ),
                // `116: agency_tool_call` -> `4: response_messages` (`ToolResponse.supplemental_media` at tag 3)
                116 => rewrite_agency_tool_call_step(arm_bytes, &mut removed),
                // `140: generic` -> `2: result` (`1: result`, `4: media`)
                140 => rewrite_generic_step(arm_bytes, &mut removed),
                _ => None,
            };
            if let Some(new_arm) = rewritten_arm {
                encode_len_delimited(f.field_number, &new_arm, &mut out);
                changed = true;
                continue;
            }
        }
        out.extend_from_slice(f.raw);
    }

    changed.then_some((out, removed))
}

fn open_trajectory_readonly(db_path: &Path) -> Option<Connection> {
    let conn = Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()?;
    let _ = conn.busy_timeout(Duration::from_millis(500));
    Some(conn)
}

fn is_safe_session_filename(session_id: &str) -> bool {
    !session_id.is_empty()
        && session_id != "."
        && session_id != ".."
        && !session_id.contains(['/', '\\', '\0'])
}

fn allocate_backup_path(db_path: &Path) -> Result<PathBuf, String> {
    let base_name = db_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| "invalid trajectory DB filename".to_string())?;
    let parent = db_path
        .parent()
        .ok_or_else(|| "trajectory DB has no parent directory".to_string())?;
    let epoch_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    for suffix in 0..1000u32 {
        let candidate_name = if suffix == 0 {
            format!("{base_name}.bak.{epoch_ms}")
        } else {
            format!("{base_name}.bak.{epoch_ms}.{suffix}")
        };
        let candidate = parent.join(candidate_name);
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err("could not allocate unique backup path for trajectory DB".into())
}

fn create_verified_sqlite_backup(src_conn: &Connection, db_path: &Path) -> Result<PathBuf, String> {
    let expected_steps: i64 = src_conn
        .query_row("SELECT count(*) FROM steps", [], |r| r.get(0))
        .map_err(|e| format!("failed to count steps before backup: {e}"))?;
    let backup_path = allocate_backup_path(db_path)?;
    let write_backup = (|| -> Result<(), String> {
        let mut dst_conn = Connection::open(&backup_path)
            .map_err(|e| format!("failed to open backup DB {}: {e}", backup_path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let _ = std::fs::set_permissions(&backup_path, std::fs::Permissions::from_mode(0o600));
        }
        {
            let backup = rusqlite::backup::Backup::new(src_conn, &mut dst_conn)
                .map_err(|e| format!("failed to initialize SQLite backup: {e}"))?;
            backup
                .run_to_completion(256, Duration::from_millis(5), None)
                .map_err(|e| format!("SQLite backup copy failed: {e}"))?;
        }
        let copied_steps: i64 = dst_conn
            .query_row("SELECT count(*) FROM steps", [], |r| r.get(0))
            .map_err(|e| format!("failed to verify backup steps table: {e}"))?;
        if copied_steps != expected_steps {
            return Err(format!(
                "backup verification mismatch: expected {expected_steps} steps, got {copied_steps}"
            ));
        }
        Ok(())
    })();
    if let Err(err) = write_backup {
        let _ = std::fs::remove_file(&backup_path);
        return Err(err);
    }
    let meta = std::fs::metadata(&backup_path)
        .map_err(|e| format!("backup file missing after completion: {e}"))?;
    if meta.len() == 0 {
        let _ = std::fs::remove_file(&backup_path);
        return Err("backup file is unexpectedly 0 bytes".into());
    }
    Ok(backup_path)
}

struct PendingMediaUpdate {
    idx: i64,
    original: Vec<u8>,
    sanitized: Vec<u8>,
}

/// Scan an Antigravity trajectory `.db` (optionally restricted to `idx > after_idx`)
/// for unreadable inline image media. When corrupt media is found:
/// 1. Creates a verified full SQLite backup (`<sid>.db.bak.<epoch_ms>`) FIRST;
///    aborts fail-closed without mutating the DB if backup fails.
/// 2. Losslessly rewrites ONLY the affected `steps.step_payload` blobs inside a
///    transaction with optimistic compare-and-swap guards.
pub(crate) fn sanitize_antigravity_trajectory_db(
    db_path: &Path,
    after_idx: Option<i64>,
) -> Result<MediaSanitizeReport, String> {
    let Ok(meta) = std::fs::metadata(db_path) else {
        return Ok(MediaSanitizeReport::default());
    };
    if !meta.is_file() || meta.len() == 0 {
        return Ok(MediaSanitizeReport::default());
    }
    let Some(read_conn) = open_trajectory_readonly(db_path) else {
        return Ok(MediaSanitizeReport::default());
    };

    let mut pending_updates = Vec::new();
    let mut removed_items: Vec<RemovedMediaItem> = Vec::new();
    {
        let query = if after_idx.is_some() {
            "SELECT idx, step_payload FROM steps WHERE idx > ?1 ORDER BY idx"
        } else {
            "SELECT idx, step_payload FROM steps ORDER BY idx"
        };
        let mut stmt = read_conn
            .prepare(query)
            .map_err(|e| format!("failed to prepare steps scan: {e}"))?;
        let mut collect_row = |idx: i64, payload: Vec<u8>| {
            if let Some((sanitized, diags)) = rewrite_step_corrupt_media(&payload) {
                for d in diags {
                    removed_items.push(RemovedMediaItem {
                        step_idx: idx,
                        mime_type: d.mime_type,
                        byte_len: d.byte_len,
                        reason: d.reason,
                    });
                }
                pending_updates.push(PendingMediaUpdate {
                    idx,
                    original: payload,
                    sanitized,
                });
            }
        };
        if let Some(min_idx) = after_idx {
            let rows = stmt
                .query_map([min_idx], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
                })
                .map_err(|e| format!("failed to query steps after {min_idx}: {e}"))?;
            for row in rows {
                let (idx, payload) = row.map_err(|e| format!("failed reading step row: {e}"))?;
                collect_row(idx, payload);
            }
        } else {
            let rows = stmt
                .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)))
                .map_err(|e| format!("failed to query steps: {e}"))?;
            for row in rows {
                let (idx, payload) = row.map_err(|e| format!("failed reading step row: {e}"))?;
                collect_row(idx, payload);
            }
        }
    }

    if pending_updates.is_empty() {
        return Ok(MediaSanitizeReport::default());
    }

    // Mandatory backup BEFORE opening a write transaction on the live trajectory DB.
    let backup_path = create_verified_sqlite_backup(&read_conn, db_path)?;
    drop(read_conn);

    apply_trajectory_media_updates(db_path, &pending_updates)?;

    Ok(MediaSanitizeReport {
        backup_path: Some(backup_path),
        removed: removed_items,
    })
}

fn apply_trajectory_media_updates(
    db_path: &Path,
    pending_updates: &[PendingMediaUpdate],
) -> Result<(), String> {
    let mut rw_conn = Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| format!("failed to open trajectory DB for repair: {e}"))?;
    let _ = rw_conn.busy_timeout(Duration::from_secs(2));
    let tx = rw_conn
        .transaction()
        .map_err(|e| format!("failed to start trajectory repair transaction: {e}"))?;
    for update in pending_updates {
        let changed = tx
            .execute(
                "UPDATE steps SET step_payload = ?1 WHERE idx = ?2 AND step_payload = ?3",
                rusqlite::params![update.sanitized, update.idx, update.original],
            )
            .map_err(|e| format!("failed to update step {}: {e}", update.idx))?;
        if changed != 1 {
            return Err(format!(
                "trajectory step {} changed during media repair; no repairs committed",
                update.idx
            ));
        }
    }
    tx.commit()
        .map_err(|e| format!("failed to commit trajectory repair: {e}"))?;
    let _ = rw_conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");

    Ok(())
}

/// Fast read-only probe checking whether any `steps` row (optionally strictly
/// after `after_idx`) contains unreadable inline image media, without creating
/// a backup or opening a write handle.
pub(crate) fn has_corrupt_antigravity_session_media(
    dir: &Path,
    session_id: &str,
    after_idx: Option<i64>,
) -> bool {
    if !is_safe_session_filename(session_id) {
        return false;
    }
    let db_path = dir.join(format!("{session_id}.db"));
    let Ok(meta) = std::fs::metadata(&db_path) else {
        return false;
    };
    if !meta.is_file() || meta.len() == 0 {
        return false;
    }
    let Some(conn) = open_trajectory_readonly(&db_path) else {
        return false;
    };
    let query = if after_idx.is_some() {
        "SELECT step_payload FROM steps WHERE idx > ?1 ORDER BY idx"
    } else {
        "SELECT step_payload FROM steps ORDER BY idx"
    };
    let Ok(mut stmt) = conn.prepare(query) else {
        return false;
    };
    if let Some(min_idx) = after_idx {
        let Ok(rows) = stmt.query_map([min_idx], |r| r.get::<_, Vec<u8>>(0)) else {
            return false;
        };
        rows.flatten()
            .any(|payload| rewrite_step_corrupt_media(&payload).is_some())
    } else {
        let Ok(rows) = stmt.query_map([], |r| r.get::<_, Vec<u8>>(0)) else {
            return false;
        };
        rows.flatten()
            .any(|payload| rewrite_step_corrupt_media(&payload).is_some())
    }
}

pub(crate) fn sanitize_antigravity_session_media(
    dir: &Path,
    session_id: &str,
) -> Result<MediaSanitizeReport, String> {
    if !is_safe_session_filename(session_id) {
        return Ok(MediaSanitizeReport::default());
    }
    sanitize_antigravity_trajectory_db(&dir.join(format!("{session_id}.db")), None)
}

pub(crate) fn sanitize_antigravity_session_media_after(
    dir: &Path,
    session_id: &str,
    after_idx: Option<i64>,
) -> Result<MediaSanitizeReport, String> {
    if !is_safe_session_filename(session_id) {
        return Ok(MediaSanitizeReport::default());
    }
    sanitize_antigravity_trajectory_db(&dir.join(format!("{session_id}.db")), after_idx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn encode_png_rgba(width: u32, height: u32) -> Vec<u8> {
        let img = image::RgbaImage::from_pixel(width, height, image::Rgba([12, 34, 56, 255]));
        let mut out = Vec::new();
        img.write_to(&mut Cursor::new(&mut out), image::ImageFormat::Png)
            .unwrap();
        out
    }

    fn make_png_chunk(chunk_type: &[u8; 4], data: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(12 + data.len());
        out.extend_from_slice(&(data.len() as u32).to_be_bytes());
        out.extend_from_slice(chunk_type);
        out.extend_from_slice(data);
        let crc = crc32_ieee(&out[4..]);
        out.extend_from_slice(&crc.to_be_bytes());
        out
    }

    fn truncated_large_png() -> Vec<u8> {
        let mut ihdr = Vec::with_capacity(13);
        ihdr.extend_from_slice(&5760u32.to_be_bytes());
        ihdr.extend_from_slice(&3240u32.to_be_bytes());
        ihdr.extend_from_slice(&[8, 6, 0, 0, 0]);
        let mut out = b"\x89PNG\r\n\x1a\n".to_vec();
        out.extend_from_slice(&make_png_chunk(b"IHDR", &ihdr));
        out.extend_from_slice(&make_png_chunk(b"IDAT", &[0x78, 0x9c, 0x01, 0x02]));
        out
    }

    fn encode_media_wire(mime_type: &str, inline_data: &[u8], blobref: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        if !mime_type.is_empty() {
            encode_len_delimited(1, mime_type.as_bytes(), &mut out);
        }
        if !inline_data.is_empty() {
            encode_len_delimited(2, inline_data, &mut out);
        }
        if !blobref.is_empty() {
            encode_len_delimited(3, blobref, &mut out);
        }
        out
    }

    #[test]
    fn validate_image_media_bytes_detects_truncation_crc_prefix_and_accepts_valid() {
        let valid_png = encode_png_rgba(4, 4);
        assert!(validate_image_media_bytes("image/png", &valid_png).is_ok());

        let trunc = truncated_large_png();
        let err = validate_image_media_bytes("image/png", &trunc).unwrap_err();
        assert!(err.contains("IEND") || err.contains("truncated"), "{err}");

        let mut bad_crc = valid_png.clone();
        let last = bad_crc.len() - 1;
        bad_crc[last] ^= 0xFF;
        let err_crc = validate_image_media_bytes("image/png", &bad_crc).unwrap_err();
        assert!(err_crc.contains("CRC"), "{err_crc}");

        let mut prefixed = b"Warning: stderr leaked\n".to_vec();
        prefixed.extend_from_slice(&valid_png);
        let err_prefix = validate_image_media_bytes("image/png", &prefixed).unwrap_err();
        assert!(err_prefix.contains("non-PNG prefix"), "{err_prefix}");
    }

    #[test]
    fn blobref_without_inline_data_and_unsupported_formats_are_not_removed() {
        let blobref_media = encode_media_wire("image/png", &[], b"blob://sha256/abc");
        assert!(check_media_proto_wire(&blobref_media).is_none());

        let svg_media = encode_media_wire("image/svg+xml", b"<svg></svg>", &[]);
        assert!(check_media_proto_wire(&svg_media).is_none());
    }

    #[test]
    fn rpc_error_info_extracts_invalid_media_2011_and_claude_error() {
        let detail = r#"{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"INVALID_MEDIA","domain":"cloudcode-pa.googleapis.com","metadata":{"error_number":"2011"}}"#;
        let info = extract_rpc_error_info(&[detail.into()], "", "", "").unwrap();
        assert!(info.is_invalid_media());
        assert_eq!(info.prefix_badge().as_deref(), Some("INVALID_MEDIA (error 2011)"));

        let decorated = decorate_with_rpc_info("INVALID_ARGUMENT (code 400)", Some(&info));
        assert!(decorated.contains("INVALID_MEDIA (error 2011)"), "{decorated}");

        assert!(is_invalid_media_error_text(
            "INVALID_ARGUMENT (code 400): invalid_request_error: Could not process image"
        ));
    }

    #[test]
    fn sanitize_db_backs_up_strips_corrupt_preserves_valid_and_cas_guards_concurrent_writes() {
        let dir = std::env::temp_dir().join(format!("waku-age173-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("sess-corrupt.db");

        let valid_png = encode_png_rgba(2, 2);
        let corrupt_png = truncated_large_png();

        // Step 0: Generic step (tag 140) with valid + corrupt media and an unknown step extension field (tag 155)
        let mut gen_res = Vec::new();
        encode_len_delimited(1, b"read_file ok", &mut gen_res);
        encode_len_delimited(4, &encode_media_wire("image/png", &valid_png, &[]), &mut gen_res);
        encode_len_delimited(4, &encode_media_wire("image/png", &corrupt_png, &[]), &mut gen_res);
        let mut gen_step = Vec::new();
        encode_len_delimited(2, &gen_res, &mut gen_step);
        let mut step0 = Vec::new();
        encode_len_delimited(140, &gen_step, &mut step0);
        encode_len_delimited(155, b"keep-unknown-ext", &mut step0);

        // Step 1: AgencyToolCall (tag 116) wrapping ToolResponse with valid JSON + corrupt supplemental_media
        let mut tool_resp = Vec::new();
        encode_len_delimited(2, br#"{"ok":true}"#, &mut tool_resp);
        encode_len_delimited(3, &encode_media_wire("image/png", &corrupt_png, &[]), &mut tool_resp);
        let mut any_msg = Vec::new();
        encode_len_delimited(
            1,
            format!("type.googleapis.com/{HARNESS_TOOL_RESPONSE_TYPE}").as_bytes(),
            &mut any_msg,
        );
        encode_len_delimited(2, &tool_resp, &mut any_msg);
        let mut agency = Vec::new();
        encode_len_delimited(4, &any_msg, &mut agency);
        let mut step1 = Vec::new();
        encode_len_delimited(116, &agency, &mut step1);

        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE steps (idx INTEGER PRIMARY KEY, step_payload BLOB NOT NULL);",
        )
        .unwrap();
        conn.execute("INSERT INTO steps (idx, step_payload) VALUES (0, ?1)", [&step0])
            .unwrap();
        conn.execute("INSERT INTO steps (idx, step_payload) VALUES (1, ?1)", [&step1])
            .unwrap();
        drop(conn);

        assert!(has_corrupt_antigravity_session_media(&dir, "sess-corrupt", None));
        let report = sanitize_antigravity_session_media(&dir, "sess-corrupt").unwrap();
        assert_eq!(report.removed.len(), 2);
        let backup_path = report.backup_path.as_ref().expect("backup created");
        assert!(backup_path.exists());
        assert!(!has_corrupt_antigravity_session_media(&dir, "sess-corrupt", None));

        // Verify unknown field + valid PNG + JSON response preserved
        let conn = Connection::open(&db_path).unwrap();
        let new_step0: Vec<u8> = conn
            .query_row("SELECT step_payload FROM steps WHERE idx = 0", [], |r| r.get(0))
            .unwrap();
        assert!(
            new_step0.windows(b"keep-unknown-ext".len()).any(|w| w == b"keep-unknown-ext"),
            "unknown protobuf field must survive"
        );
        assert!(
            new_step0.windows(valid_png.len()).any(|w| w == valid_png.as_slice()),
            "valid PNG must survive"
        );
        assert!(
            !new_step0.windows(corrupt_png.len()).any(|w| w == corrupt_png.as_slice()),
            "corrupt PNG must be stripped"
        );
        let new_step1: Vec<u8> = conn
            .query_row("SELECT step_payload FROM steps WHERE idx = 1", [], |r| r.get(0))
            .unwrap();
        assert!(
            new_step1.windows(br#"{"ok":true}"#.len()).any(|w| w == br#"{"ok":true}"#),
            "ToolResponse JSON must stay intact"
        );

        // Verify CAS rejects concurrent write
        let stale_update = PendingMediaUpdate {
            idx: 0,
            original: step0,
            sanitized: vec![1, 2, 3],
        };
        let err = apply_trajectory_media_updates(&db_path, &[stale_update]).unwrap_err();
        assert!(err.contains("changed during media repair"), "{err}");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
