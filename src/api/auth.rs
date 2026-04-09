//! Authentication and cryptographic signing for Polymarket API.
//!
//! Handles:
//! - ECDSA signing for order creation
//! - API key derivation from private key
//! - HMAC signing for REST API authentication
//! - EIP-712 typed data signing

use crate::core::error::{Error, Result};
use chrono::Utc;
use ethers::{
    core::k256::ecdsa::SigningKey,
    signers::{LocalWallet, Signer as EthersSigner, Wallet},
    types::{Address, Signature, H256},
    utils::keccak256,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::str::FromStr;
use std::sync::Arc;

type HmacSha256 = Hmac<Sha256>;

/// Signer for Polymarket API authentication
#[derive(Clone)]
pub struct Signer {
    wallet: Arc<LocalWallet>,
    api_key: String,
    api_secret: String,
    api_passphrase: String,
}

impl Signer {
    /// Create a new signer from a private key
    pub fn new(private_key: &str) -> Result<Self> {
        let private_key = private_key.strip_prefix("0x").unwrap_or(private_key);

        let wallet: LocalWallet = private_key
            .parse()
            .map_err(|e| Error::Crypto(format!("Invalid private key: {}", e)))?;

        // Derive API credentials from the wallet
        let (api_key, api_secret, api_passphrase) = Self::derive_api_credentials(&wallet)?;

        Ok(Self {
            wallet: Arc::new(wallet),
            api_key,
            api_secret,
            api_passphrase,
        })
    }

    /// Create a signer with explicit API credentials
    pub fn with_credentials(
        private_key: &str,
        api_key: String,
        api_secret: String,
        api_passphrase: String,
    ) -> Result<Self> {
        let private_key = private_key.strip_prefix("0x").unwrap_or(private_key);

        let wallet: LocalWallet = private_key
            .parse()
            .map_err(|e| Error::Crypto(format!("Invalid private key: {}", e)))?;

        Ok(Self {
            wallet: Arc::new(wallet),
            api_key,
            api_secret,
            api_passphrase,
        })
    }

    /// Derive API credentials from wallet
    fn derive_api_credentials(wallet: &LocalWallet) -> Result<(String, String, String)> {
        // Sign a deterministic message to derive API key
        let message = "POLYMARKET_API_KEY_DERIVATION";
        let signature = Self::sign_message_sync(wallet, message)?;

        // Use parts of the signature as API credentials
        let sig_bytes = signature.to_vec();

        // API key: first 16 bytes hex-encoded
        let api_key = hex::encode(&sig_bytes[0..16]);

        // API secret: next 32 bytes base64-encoded
        let api_secret = base64::encode(&sig_bytes[16..48]);

        // API passphrase: derived from r value
        let passphrase_hash = keccak256(&sig_bytes[48..64]);
        let api_passphrase = hex::encode(&passphrase_hash[0..8]);

        Ok((api_key, api_secret, api_passphrase))
    }

    /// Sign a message synchronously (for derivation)
    fn sign_message_sync(wallet: &LocalWallet, message: &str) -> Result<Signature> {
        let message_hash = keccak256(format!("\x19Ethereum Signed Message:\n{}{}", message.len(), message).as_bytes());

        let signing_key = wallet.signer();
        let (sig, recovery_id) = signing_key
            .sign_prehash_recoverable(&message_hash)
            .map_err(|e| Error::Crypto(format!("Failed to sign: {}", e)))?;

        let r = H256::from_slice(&sig.r().to_bytes());
        let s = H256::from_slice(&sig.s().to_bytes());
        let v = recovery_id.to_byte() as u64 + 27;

        Ok(Signature { r, s, v })
    }

    /// Get the wallet address
    pub fn address(&self) -> Address {
        self.wallet.address()
    }

    /// Get the address as a string
    pub fn address_string(&self) -> String {
        format!("{:?}", self.wallet.address())
    }

    /// Get the API key
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Get the API secret
    pub fn api_secret(&self) -> &str {
        &self.api_secret
    }

    /// Get the API passphrase
    pub fn api_passphrase(&self) -> &str {
        &self.api_passphrase
    }

    /// Sign a message (EIP-191 personal sign)
    pub async fn sign_message(&self, message: &str) -> Result<String> {
        let signature = self
            .wallet
            .sign_message(message)
            .await
            .map_err(|e| Error::Crypto(format!("Failed to sign message: {}", e)))?;

        Ok(format!("0x{}", hex::encode(signature.to_vec())))
    }

    /// Sign a hash directly
    pub async fn sign_hash(&self, hash: H256) -> Result<Signature> {
        let signing_key = self.wallet.signer();
        let (sig, recovery_id) = signing_key
            .sign_prehash_recoverable(hash.as_bytes())
            .map_err(|e| Error::Crypto(format!("Failed to sign hash: {}", e)))?;

        let r = H256::from_slice(&sig.r().to_bytes());
        let s = H256::from_slice(&sig.s().to_bytes());
        let v = recovery_id.to_byte() as u64 + 27;

        Ok(Signature { r, s, v })
    }

    /// Generate HMAC signature for REST API requests
    pub fn sign_request(
        &self,
        method: &str,
        path: &str,
        timestamp: &str,
        body: &str,
    ) -> Result<String> {
        let message = format!("{}{}{}{}", timestamp, method.to_uppercase(), path, body);

        let secret_decoded = base64::decode(&self.api_secret)
            .map_err(|e| Error::Crypto(format!("Invalid API secret: {}", e)))?;

        let mut mac = HmacSha256::new_from_slice(&secret_decoded)
            .map_err(|e| Error::Crypto(format!("Invalid HMAC key: {}", e)))?;

        mac.update(message.as_bytes());
        let result = mac.finalize();

        Ok(base64::encode(result.into_bytes()))
    }

    /// Generate authentication headers for CLOB API
    pub fn auth_headers(&self, method: &str, path: &str, body: &str) -> Result<AuthHeaders> {
        let timestamp = Utc::now().timestamp_millis().to_string();
        let signature = self.sign_request(method, path, &timestamp, body)?;

        Ok(AuthHeaders {
            api_key: self.api_key.clone(),
            signature,
            timestamp,
            passphrase: self.api_passphrase.clone(),
        })
    }

    /// Sign order data for CLOB submission
    pub async fn sign_order(&self, order_data: &OrderSigningData) -> Result<String> {
        // Create EIP-712 typed data hash
        let domain_separator = self.compute_domain_separator(order_data.chain_id);
        let struct_hash = self.compute_order_struct_hash(order_data);

        let digest = keccak256(
            [
                &[0x19, 0x01][..],
                domain_separator.as_bytes(),
                struct_hash.as_bytes(),
            ]
            .concat(),
        );

        let signature = self.sign_hash(H256::from(digest)).await?;

        // Return signature in the format expected by Polymarket
        Ok(format!(
            "0x{}{}{}",
            hex::encode(signature.r),
            hex::encode(signature.s),
            format!("{:02x}", signature.v)
        ))
    }

    /// Compute EIP-712 domain separator
    fn compute_domain_separator(&self, chain_id: u64) -> H256 {
        let type_hash = keccak256(
            b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)",
        );

        let name_hash = keccak256(b"Polymarket CTF Exchange");
        let version_hash = keccak256(b"1");

        let encoded = ethers::abi::encode(&[
            ethers::abi::Token::FixedBytes(type_hash.to_vec()),
            ethers::abi::Token::FixedBytes(name_hash.to_vec()),
            ethers::abi::Token::FixedBytes(version_hash.to_vec()),
            ethers::abi::Token::Uint(chain_id.into()),
            ethers::abi::Token::Address(
                Address::from_str(crate::core::constants::CTF_EXCHANGE_ADDRESS)
                    .expect("Invalid CTF exchange address"),
            ),
        ]);

        H256::from(keccak256(&encoded))
    }

    /// Compute order struct hash for EIP-712
    fn compute_order_struct_hash(&self, order: &OrderSigningData) -> H256 {
        let type_hash = keccak256(
            b"Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)",
        );

        let encoded = ethers::abi::encode(&[
            ethers::abi::Token::FixedBytes(type_hash.to_vec()),
            ethers::abi::Token::Uint(order.salt.into()),
            ethers::abi::Token::Address(order.maker),
            ethers::abi::Token::Address(order.signer),
            ethers::abi::Token::Address(order.taker.unwrap_or(Address::zero())),
            ethers::abi::Token::Uint(order.token_id.clone().into()),
            ethers::abi::Token::Uint(order.maker_amount.into()),
            ethers::abi::Token::Uint(order.taker_amount.into()),
            ethers::abi::Token::Uint(order.expiration.into()),
            ethers::abi::Token::Uint(order.nonce.into()),
            ethers::abi::Token::Uint(order.fee_rate_bps.into()),
            ethers::abi::Token::Uint(order.side.into()),
            ethers::abi::Token::Uint(order.signature_type.into()),
        ]);

        H256::from(keccak256(&encoded))
    }
}

impl std::fmt::Debug for Signer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Signer")
            .field("address", &self.address_string())
            .field("api_key", &"[REDACTED]")
            .finish()
    }
}

/// Authentication headers for API requests
#[derive(Debug, Clone)]
pub struct AuthHeaders {
    pub api_key: String,
    pub signature: String,
    pub timestamp: String,
    pub passphrase: String,
}

/// Data required for signing an order
#[derive(Debug, Clone)]
pub struct OrderSigningData {
    pub salt: u128,
    pub maker: Address,
    pub signer: Address,
    pub taker: Option<Address>,
    pub token_id: ethers::types::U256,
    pub maker_amount: u128,
    pub taker_amount: u128,
    pub expiration: u64,
    pub nonce: u64,
    pub fee_rate_bps: u64,
    pub side: u8,
    pub signature_type: u8,
    pub chain_id: u64,
}

impl OrderSigningData {
    /// Create signing data for a buy order
    pub fn buy(
        maker: Address,
        signer: Address,
        token_id: &str,
        price: rust_decimal::Decimal,
        quantity: rust_decimal::Decimal,
        expiration: u64,
        nonce: u64,
        chain_id: u64,
    ) -> Result<Self> {
        let token_id = ethers::types::U256::from_dec_str(token_id)
            .map_err(|e| Error::Parse(format!("Invalid token ID: {}", e)))?;

        // For buy: maker provides USDC, taker provides tokens
        // maker_amount = price * quantity (in USDC units, 6 decimals)
        // taker_amount = quantity (in token units, 6 decimals)
        let usdc_amount = (price * quantity * rust_decimal::Decimal::from(1_000_000))
            .to_string()
            .parse::<u128>()
            .map_err(|e| Error::Parse(format!("Invalid amount: {}", e)))?;

        let token_amount = (quantity * rust_decimal::Decimal::from(1_000_000))
            .to_string()
            .parse::<u128>()
            .map_err(|e| Error::Parse(format!("Invalid quantity: {}", e)))?;

        Ok(Self {
            salt: rand::random(),
            maker,
            signer,
            taker: None,
            token_id,
            maker_amount: usdc_amount,
            taker_amount: token_amount,
            expiration,
            nonce,
            fee_rate_bps: 0,
            side: 0, // 0 = buy
            signature_type: 0,
            chain_id,
        })
    }

    /// Create signing data for a sell order
    pub fn sell(
        maker: Address,
        signer: Address,
        token_id: &str,
        price: rust_decimal::Decimal,
        quantity: rust_decimal::Decimal,
        expiration: u64,
        nonce: u64,
        chain_id: u64,
    ) -> Result<Self> {
        let token_id = ethers::types::U256::from_dec_str(token_id)
            .map_err(|e| Error::Parse(format!("Invalid token ID: {}", e)))?;

        // For sell: maker provides tokens, taker provides USDC
        let token_amount = (quantity * rust_decimal::Decimal::from(1_000_000))
            .to_string()
            .parse::<u128>()
            .map_err(|e| Error::Parse(format!("Invalid quantity: {}", e)))?;

        let usdc_amount = (price * quantity * rust_decimal::Decimal::from(1_000_000))
            .to_string()
            .parse::<u128>()
            .map_err(|e| Error::Parse(format!("Invalid amount: {}", e)))?;

        Ok(Self {
            salt: rand::random(),
            maker,
            signer,
            taker: None,
            token_id,
            maker_amount: token_amount,
            taker_amount: usdc_amount,
            expiration,
            nonce,
            fee_rate_bps: 0,
            side: 1, // 1 = sell
            signature_type: 0,
            chain_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test private key (DO NOT USE IN PRODUCTION)
    const TEST_PRIVATE_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    #[test]
    fn test_signer_creation() {
        let signer = Signer::new(TEST_PRIVATE_KEY).expect("Failed to create signer");
        assert!(!signer.api_key().is_empty());
        assert!(!signer.address_string().is_empty());
    }

    #[test]
    fn test_request_signing() {
        let signer = Signer::new(TEST_PRIVATE_KEY).expect("Failed to create signer");

        let headers = signer
            .auth_headers("POST", "/order", r#"{"test": "data"}"#)
            .expect("Failed to generate auth headers");

        assert!(!headers.signature.is_empty());
        assert!(!headers.timestamp.is_empty());
    }

    #[tokio::test]
    async fn test_message_signing() {
        let signer = Signer::new(TEST_PRIVATE_KEY).expect("Failed to create signer");

        let signature = signer
            .sign_message("test message")
            .await
            .expect("Failed to sign message");

        assert!(signature.starts_with("0x"));
        assert_eq!(signature.len(), 132); // 0x + 130 hex chars
    }
}
