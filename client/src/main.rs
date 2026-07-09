use std::{
    fmt::Debug,
    fs::File,
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, TcpStream, UdpSocket},
    time::{Duration, Instant},
};

use clap::Parser;
use common::{StockQuote, VALID_TICKERS};
use log::{debug, info, trace};

const DEFAULT_PING_TIMEOUT_SECS: u64 = 5;

#[derive(Parser)]
#[command(name = "client")]
#[command(version = "1.0")]
pub struct Args {
    /// Адрес сервера
    #[arg(long, value_name = "server")]
    server: String,
    /// UDP адрес приема котировок
    #[arg(long, value_name = "udp")]
    udp: String,
    /// Таймаут пинга в секундах
    #[arg(long, value_name = "ping_timeout_secs", default_value_t = DEFAULT_PING_TIMEOUT_SECS )]
    timeout: u64,
    /// Имя файла с тикерами
    #[arg(long, value_name = "tickers")]
    tickers: String,
}

enum AppError {
    FileOpen,
    FileRead,
    WrongTicker(String),
    UdpError(String),
    TcpConnectError(String),
    TcpSendError,
    TcpReceiveError,
}

impl Debug for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppError::FileOpen => writeln!(f, "Файл с тикерами не найден"),
            AppError::FileRead => writeln!(f, "Ошибка чтения файла с тикерами"),
            AppError::WrongTicker(str) => writeln!(f, "Неверный тикер {}", str),
            AppError::UdpError(str) => writeln!(f, "Ошибка UDP ({})", str),
            AppError::TcpConnectError(str) => writeln!(f, "Ошибка подключения по TCP ({})", str),
            AppError::TcpSendError => writeln!(f, "Ошибка отправки данных по TCP"),
            AppError::TcpReceiveError => writeln!(f, "Ошибка получения данных по TCP"),
        }
    }
}

fn main() -> Result<(), AppError> {
    env_logger::init();

    let args = Args::parse();

    let file = File::open(args.tickers).map_err(|_| AppError::FileOpen)?;
    let reader = BufReader::new(file);

    let mut tickers: Vec<&'static str> = Vec::new();

    for line in reader.lines() {
        match line {
            Ok(line) => match VALID_TICKERS.get(line.as_str()) {
                Some(ticker) => tickers.push(*ticker),
                None => Err(AppError::WrongTicker(line))?,
            },
            Err(_) => Err(AppError::FileRead)?,
        }
    }
    trace!("Считаны тикеры: {}", tickers.len());

    let udp = UdpSocket::bind(&args.udp).map_err(|x| AppError::UdpError(x.to_string()))?;
    info!("UDP слушает {}", args.udp);

    let req = format!("STREAM {} {}\n", args.udp, tickers.join(", "));

    let mut stream =
        TcpStream::connect(&args.server).map_err(|x| AppError::TcpConnectError(x.to_string()))?;
    info!("Подключен к {}", &args.server);

    stream
        .write_all(req.as_bytes())
        .map_err(|_| AppError::TcpSendError)?;
    trace!("Послали команду {}", &req);

    let mut reader = BufReader::new(stream);

    let mut response: String = String::new();

    reader
        .read_line(&mut response)
        .map_err(|_| AppError::TcpReceiveError)?;

    trace!("Получили ответ {}", &response);

    if response == "OK\n" {
        udp.set_read_timeout(Some(Duration::from_secs(1)))
            .map_err(|_| AppError::UdpError("Не могу установить таймаут UDP".to_string()))?;
        let ping_duration = Duration::from_secs(args.timeout);
        let mut ping_time = Instant::now() + ping_duration;
        let mut buffer: [u8; _] = [0; 1024];
        let ping: [u8; _] = [b'P', b'I', b'N', b'G'];
        let mut quote_sender: Option<SocketAddr> = None;
        loop {
            match udp.recv_from(&mut buffer) {
                Ok((bytes, sender)) => {
                    quote_sender = Some(sender);
                    match String::from_utf8(buffer[..bytes].to_vec()) {
                        Ok(data) => match StockQuote::from_wire_line(&data) {
                            Ok(quote) => println!("Получили котировку {}", quote),
                            Err(_) => debug!("Ошибка десериализации"),
                        },
                        Err(_) => debug!("Невадидная сторка"),
                    }
                }
                Err(e) => {
                    match e.kind() {
                        std::io::ErrorKind::TimedOut => {} //Не ошибка,
                        _ => debug!("Ошибка UDP {}", e),
                    }
                }
            }
            if ping_time <= Instant::now()
                && let Some(ping_receiver) = quote_sender
            {
                ping_time = Instant::now() + ping_duration;
                match udp.send_to(&ping, ping_receiver) {
                    Ok(_) => debug!("Пинг послан"),
                    Err(e) => debug!("Ошибка при отправке пинга {}", e),
                }
            }
        }
    }

    Ok(())
}
