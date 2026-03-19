use crate::AppConfig;
use anyhow::Result;
use base64::Engine;
use rand::thread_rng;
use rsa::{
    RsaPrivateKey, pkcs1::DecodeRsaPrivateKey, pkcs8::DecodePrivateKey, pss::BlindedSigningKey,
};
use sha2::Sha256;
use signature::{RandomizedSigner, SignatureEncoding};
use std::{fs, path::Path};

pub fn api_key_id(config: &AppConfig) -> Result<Option<String>> {
    if let Some(value) = config
        .kalshi_api_key_id
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        return Ok(Some(value.to_string()));
    }
    let Some(path) = config.kalshi_api_key_id_file.as_ref() else {
        return Ok(None);
    };
    let value = fs::read_to_string(path)?.trim().to_string();
    if value.is_empty() {
        return Ok(None);
    }
    Ok(Some(value))
}

pub fn private_key(config: &AppConfig) -> Result<Option<RsaPrivateKey>> {
    if let Some(raw) = config
        .kalshi_private_key_b64
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        let decoded = base64::engine::general_purpose::STANDARD.decode(raw)?;
        let pem = String::from_utf8(decoded)?;
        if let Ok(private_key) = RsaPrivateKey::from_pkcs8_pem(&pem) {
            return Ok(Some(private_key));
        }
        if let Ok(private_key) = RsaPrivateKey::from_pkcs1_pem(&pem) {
            return Ok(Some(private_key));
        }
        return Err(anyhow::anyhow!(
            "failed to parse Kalshi private key from KALSHI_PRIVATE_KEY_B64 as PKCS#8 or PKCS#1 PEM"
        ));
    }
    let Some(path) = config.kalshi_private_key_path.as_ref() else {
        return Ok(None);
    };
    let pem = fs::read_to_string(Path::new(path))?;
    if let Ok(private_key) = RsaPrivateKey::from_pkcs8_pem(&pem) {
        return Ok(Some(private_key));
    }
    if let Ok(private_key) = RsaPrivateKey::from_pkcs1_pem(&pem) {
        return Ok(Some(private_key));
    }
    Err(anyhow::anyhow!(
        "failed to parse Kalshi private key at {} as PKCS#8 or PKCS#1 PEM",
        path
    ))
}

pub fn timestamp_ms() -> String {
    ((chrono::Utc::now().timestamp_millis()) as i128).to_string()
}

pub fn sign_request(
    private_key: &RsaPrivateKey,
    timestamp: &str,
    method: &str,
    path: &str,
) -> Result<String> {
    let message = format!(
        "{}{}{}",
        timestamp,
        method.to_uppercase(),
        signature_path(path)
    );
    let signing_key = BlindedSigningKey::<Sha256>::new(private_key.clone());
    let signature = signing_key.sign_with_rng(&mut thread_rng(), message.as_bytes());
    Ok(base64::engine::general_purpose::STANDARD.encode(signature.to_bytes()))
}

pub fn sign_ws_connect_request(
    private_key: &RsaPrivateKey,
    timestamp: &str,
    ws_path: &str,
) -> Result<String> {
    let message = format!(
        "{}GET{}",
        timestamp,
        ws_path.split('?').next().unwrap_or(ws_path)
    );
    let signing_key = BlindedSigningKey::<Sha256>::new(private_key.clone());
    let signature = signing_key.sign_with_rng(&mut thread_rng(), message.as_bytes());
    Ok(base64::engine::general_purpose::STANDARD.encode(signature.to_bytes()))
}

fn signature_path(path: &str) -> String {
    let base_path = path.split('?').next().unwrap_or(path);
    format!("/trade-api/v2{base_path}")
}
