use std::collections::HashMap;
use std::fmt::Debug;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use clap::Parser;

use common::StockQuote;
use log::{error, info};

use crate::dispatch::{DispatchMessage, Subscription, dispatch};
use crate::generator::{GeneratorError, generator};
use crate::network::{NetworkError, listen};

mod dispatch;
mod generator;
mod network;

const DEFAULT_PORT: u16 = 8080;
const DEFAULT_PING_TIMEOUT_SECS: u64 = 10;
const DEFAULT_GENERATE_MS: u64 = 500;

#[derive(Parser)]
#[command(name = "server")]
#[command(version = "1.0")]
pub struct Args {
    /// Прослушиваемый порт
    #[arg(long, value_name = "port", default_value_t = DEFAULT_PORT )]
    port: u16,
    /// Таймаут пинга в секундах
    #[arg(long, value_name = "ping_timeout_secs", default_value_t = DEFAULT_PING_TIMEOUT_SECS )]
    timeout: u64,
    /// Частота генерации котировок в мс
    #[arg(long, value_name = "generate_ms", default_value_t = DEFAULT_GENERATE_MS )]
    generate_ms: u64,
}

pub enum AppError {
    JoinError,
    CtrlCHandlerError,
    GeneratorError(GeneratorError),
    NetworkError(NetworkError),
}

impl Debug for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppError::JoinError => {
                write!(f, "Ошибка при ожидании завершения потоков")
            }
            AppError::CtrlCHandlerError => {
                write!(f, "Ошибка при установке обработчика сигналов")
            }
            AppError::GeneratorError(e) => {
                write!(f, "Ошибка генератора: {}", e)
            }
            AppError::NetworkError(e) => {
                write!(f, "Ошибка прослушивания: {}", e)
            }
        }
    }
}

fn main() -> Result<(), AppError> {
    env_logger::init();

    let args = Args::parse();

    let running = Arc::new(AtomicBool::new(true));
    let r = Arc::clone(&running);

    ctrlc::set_handler(move || {
        info!("Получен запрос на остановку");
        r.store(false, Ordering::Relaxed);
    })
    .map_err(|_| AppError::CtrlCHandlerError)?;

    let (quote_sender, quote_receiver) =
        std::sync::mpsc::channel::<HashMap<&'static str, StockQuote>>();

    let generator_r = Arc::clone(&running);
    let sleep_duration = Duration::from_millis(args.generate_ms);
    let generator_thread = std::thread::spawn(move || -> Result<(), AppError> {
        let result = generator(quote_sender, sleep_duration, Arc::clone(&generator_r));
        if generator_r.load(Ordering::Relaxed) {
            info!("Генератор остановлен. Останавливаем сервер");
            generator_r.store(false, Ordering::Relaxed);
        }
        result.map_err(|e| {
            error!("Ошибка генератора {}", e);
            AppError::GeneratorError(e)
        })
    });

    let (dispatch_sender, dispatch_receiver) = std::sync::mpsc::channel::<DispatchMessage>();
    let quote_to_dispatch_r = Arc::clone(&running);
    let quote_to_dispatch = dispatch_sender.clone();
    let quote_to_dispatch_thread = std::thread::spawn(move || {
        loop {
            if quote_to_dispatch_r.load(Ordering::Relaxed) == false {
                break;
            }
            match quote_receiver.recv_timeout(Duration::from_secs(1)) {
                Ok(msg) => match quote_to_dispatch.send(DispatchMessage::Quotes(msg)) {
                    Ok(_) => (),
                    Err(_) => break, //Умер dispatch_receiver = стоп
                },
                Err(e) => match e {
                    std::sync::mpsc::RecvTimeoutError::Timeout => continue, //Таймаут - Ок
                    std::sync::mpsc::RecvTimeoutError::Disconnected => break, //Умер quote_sender = стоп
                },
            }
        }
        if quote_to_dispatch_r.load(Ordering::Relaxed) {
            info!("quote_to_dispatch остановлен. Останавливаем сервер");
            quote_to_dispatch_r.store(false, Ordering::Relaxed);
        }
    });

    let (subscribe_sender, subscribe_receiver) = std::sync::mpsc::channel::<Subscription>();
    let subscribe_to_dispatch_r = Arc::clone(&running);
    let subscribe_to_dispatch = dispatch_sender;
    let subscribe_to_dispatch_thread = std::thread::spawn(move || {
        loop {
            if subscribe_to_dispatch_r.load(Ordering::Relaxed) == false {
                break;
            }
            match subscribe_receiver.recv_timeout(Duration::from_secs(1)) {
                Ok(msg) => match subscribe_to_dispatch.send(DispatchMessage::Subscribe(msg)) {
                    Ok(_) => (),
                    Err(_) => break, //Умер dispatch_receiver = стоп
                },
                Err(e) => match e {
                    std::sync::mpsc::RecvTimeoutError::Timeout => continue, // Таймаут - Ок
                    std::sync::mpsc::RecvTimeoutError::Disconnected => break, // Умер subscribe_sender = стоп
                },
            }
        }
        if subscribe_to_dispatch_r.load(Ordering::Relaxed) {
            info!("subscribe_to_dispatch остановлен. Останавливаем сервер");
            subscribe_to_dispatch_r.store(false, Ordering::Relaxed);
        }
    });

    let dispatch_r = Arc::clone(&running);
    let dispatch_thread = std::thread::spawn(move || {
        dispatch(dispatch_receiver, dispatch_r);
    });

    let listener_r = Arc::clone(&running);
    let listener_thread = std::thread::spawn(move || -> Result<(), AppError> {
        listen(subscribe_sender, args, listener_r).map_err(|e| {
            error!("Ошибка сети {}", e);
            AppError::NetworkError(e)
        })
    });

    while running.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_secs(1));
    }

    generator_thread.join().map_err(|_| AppError::JoinError)??;

    quote_to_dispatch_thread
        .join()
        .map_err(|_| AppError::JoinError)?;

    subscribe_to_dispatch_thread
        .join()
        .map_err(|_| AppError::JoinError)?;

    dispatch_thread.join().map_err(|_| AppError::JoinError)?;

    listener_thread.join().map_err(|_| AppError::JoinError)??;

    Ok(())
}
