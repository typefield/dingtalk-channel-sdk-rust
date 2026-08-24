//! Legacy OAPI client: gettoken cache + media upload
//! (SPEC §9a; Go `oapi.go` port, mirrors official connector media/common.ts).

use crate::config::Config;
use crate::error::{Error, Result};
use serde::Deserialize;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, serde::Serialize)]
pub struct MediaUploadResult {
    /// mediaId with leading `@` stripped.
    #[serde(rename = "mediaId")]
    pub media_id: String,
    /// Raw media_id (with `@`) — required by sampleFile/sampleVideo/sampleAudio sends.
    #[serde(rename = "rawMediaId")]
    pub raw_media_id: String,
    #[serde(rename = "type")]
    pub r#type: String,
    /// DingTalk returns a unix timestamp (number).
    #[serde(rename = "created_at")]
    pub created_at: i64,
    /// down.dingtalk.com URL (only some media are reachable; reliable image delivery uses SendFile).
    #[serde(rename = "downloadUrl")]
    pub download_url: String,
}

#[derive(Deserialize)]
struct OapiTokenResponse {
    errcode: i64,
    errmsg: Option<String>,
    #[serde(default)]
    access_token: String,
    #[serde(default)]
    expires_in: i64,
}

#[derive(Deserialize)]
struct MediaUploadResponse {
    errcode: i64,
    errmsg: Option<String>,
    #[serde(default, rename = "media_id")]
    media_id: String,
    #[serde(default)]
    r#type: String,
    #[serde(default, rename = "created_at")]
    created_at: i64,
}

/// Legacy OAPI: token cache + multipart media upload.
pub(crate) struct OapiClient {
    cfg: Arc<Config>,
    http: reqwest::Client,
    state: tokio::sync::Mutex<OapiState>,
}

#[derive(Default)]
struct OapiState {
    token: String,
    expiry: Option<Instant>,
}

impl OapiClient {
    pub(crate) fn new(cfg: Arc<Config>, http: reqwest::Client) -> Self {
        Self {
            cfg,
            http,
            state: tokio::sync::Mutex::new(OapiState::default()),
        }
    }

    async fn get_token(&self) -> Result<String> {
        let mut st = self.state.lock().await;
        if !st.token.is_empty() {
            if let Some(exp) = st.expiry {
                if Instant::now() + Duration::from_secs(60) < exp {
                    return Ok(st.token.clone());
                }
            }
        }
        let url = format!(
            "{}/gettoken?appkey={}&appsecret={}",
            self.cfg.oapi_base,
            urlencoding::encode(&self.cfg.client_id),
            urlencoding::encode(&self.cfg.client_secret),
        );
        let resp = self.http.get(url).send().await?;
        let raw = resp.text().await?;
        let out: OapiTokenResponse = serde_json::from_str(&raw)?;
        if out.errcode != 0 || out.access_token.is_empty() {
            return Err(Error::channel(format!(
                "oapi gettoken: errcode={} errmsg={}",
                out.errcode,
                out.errmsg.unwrap_or_default()
            )));
        }
        st.token = out.access_token;
        let expires_in = if out.expires_in <= 0 {
            7200
        } else {
            out.expires_in
        };
        st.expiry = Some(Instant::now() + Duration::from_secs(expires_in as u64));
        Ok(st.token.clone())
    }

    /// Upload media (multipart, field name `media`); returns mediaId with `@` stripped.
    /// `media_type`: image | file | video | voice; empty content_type infers by type.
    pub async fn upload_media(
        &self,
        media_type: &str,
        filename: &str,
        content_type: &str,
        data: Vec<u8>,
    ) -> Result<MediaUploadResult> {
        let token = self.get_token().await?;
        let ct = if content_type.is_empty() {
            if media_type == "image" {
                "image/jpeg"
            } else {
                "application/octet-stream"
            }
        } else {
            content_type
        };

        let part = reqwest::multipart::Part::bytes(data)
            .file_name(filename.to_string())
            .mime_str(ct)
            .map_err(|e| Error::channel(format!("bad mime: {e}")))?;
        let form = reqwest::multipart::Form::new().part("media", part);

        let url = format!(
            "{}/media/upload?access_token={}&type={}",
            self.cfg.oapi_base,
            urlencoding::encode(&token),
            urlencoding::encode(media_type),
        );
        let resp = self.http.post(url).multipart(form).send().await?;
        let raw = resp.text().await?;
        let out: MediaUploadResponse = serde_json::from_str(&raw)?;
        if out.errcode != 0 {
            return Err(Error::channel(format!(
                "media/upload: errcode={} errmsg={}",
                out.errcode,
                out.errmsg.unwrap_or_default()
            )));
        }
        let raw_id = out.media_id;
        let media_id = raw_id.strip_prefix('@').unwrap_or(&raw_id).to_string(); // connector-parity cleanup
        Ok(MediaUploadResult {
            download_url: format!("https://down.dingtalk.com/media/{media_id}"),
            media_id,
            raw_media_id: raw_id,
            r#type: out.r#type,
            created_at: out.created_at,
        })
    }
}
