//! Core module containing fundamental types and traits for the HFT arbitrage bot.
//!
//! This module provides:
//! - Common types used throughout the application
//! - Configuration management
//! - Error handling
//! - Event system for communication between components

pub mod types;
pub mod config;
pub mod error;
pub mod events;
pub mod constants;

pub use types::*;
pub use config::Config;
pub use error::{Error, Result};
pub use events::{Event, EventBus};
pub use constants::*;
