use crate::{Args, dispatch::Subscription};
use common::VALID_TICKERS;
use log::{debug, info, trace, warn};
use std::fmt::Display;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{atomic::AtomicBool, mpsc::Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub enum NetworkError {
    ListenError,
    SetNonblocking,
    AcceptError,
    SetTimeout,
    CommandReceive,
}

impl Display for NetworkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ListenError => write!(f, "ListenError"),
            Self::SetNonblocking => write!(f, "SetNonblocking"),
            Self::AcceptError => write!(f, "AcceptError"),
            Self::SetTimeout => write!(f, "SetTimeout"),
            Self::CommandReceive => write!(f, "CommandReceive"),
        }
    }
}

struct Command {
    response: String,
    join: Option<JoinHandle<Result<(), String>>>,
}

fn udp_thread(
    udp_addr: &str,
    udp_socket: UdpSocket,
    receiver: Receiver<Vec<u8>>,
    timeout: Duration,
    run: Arc<AtomicBool>,
) -> Result<(), String> {
    let mut buf: [u8; _] = [0; 128];
    let mut last_ping_time = Instant::now();
    loop {
        if run.load(Ordering::Relaxed) == false {
            break;
        }
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(msg) => {
                debug!("Отправляем {} байт на {}", msg.len(), udp_addr);
                trace!("{:?}", &msg);
                udp_socket
                    .send(&msg)
                    .map_err(|err| format!("Ошибка {} при отправке на {}", err, udp_addr))?;
            }
            Err(err) => match err {
                RecvTimeoutError::Timeout => {}
                RecvTimeoutError::Disconnected => break,
            },
        }
        if let Ok(bytes) = udp_socket.recv(&mut buf)
            && bytes == 4
            && buf[0..4].eq_ignore_ascii_case(b"PING")
        {
            debug!("Пинг от {} получен", &udp_addr);
            last_ping_time = Instant::now();
        }
        if Instant::now() - last_ping_time > timeout {
            debug!("Ping для {} не пришёл, прекращаем трансляцию", &udp_addr);
            break;
        }
    }
    Ok(())
}

fn process_command(
    command: &str,
    subscription_sender: Sender<Subscription>,
    timeout: Duration,
    run: Arc<AtomicBool>,
) -> Result<Command, String> {
    let split: Vec<_> = command.splitn(3, " ").collect();

    if split.len() < 2 || split[0] != "STREAM" {
        return Ok(Command {
            response: "ERR invalid command\n".to_string(),
            join: None,
        });
    }

    let udp =
        UdpSocket::bind("0.0.0.0:0").map_err(|e| format!("Не могу создать UDP сокет {}", e))?;

    udp.set_nonblocking(true)
        .map_err(|e| format!("Не могу установить nonblocking {}", e))?;

    let udp_addr = split[1].trim().to_string();

    if udp.connect(&udp_addr).is_err() {
        return Ok(Command {
            response: "ERR invalid udp address\n".to_string(),
            join: None,
        });
    }

    if (split.len()) == 2 {
        return Ok(Command {
            response: "ERR empty ticker list\n".to_string(),
            join: None,
        });
    }

    let tickers = split[2].replace(" ", "");
    let tickers: Vec<_> = tickers.trim().split(",").collect();

    let mut ticker_list: Vec<&'static str> = Vec::new();

    for ticker in tickers {
        match VALID_TICKERS.get(ticker) {
            Some(x) => ticker_list.push(*x),
            None => {
                return Ok(Command {
                    response: format!("ERR unknown ticker {}\n", ticker),
                    join: None,
                });
            }
        }
    }

    let (quote_sender, quote_receiver) = std::sync::mpsc::channel::<Vec<u8>>();

    debug!(
        "Подписываем {} на {} тикеров",
        &udp_addr,
        &ticker_list.len()
    );
    trace!("{:?}", &ticker_list);
    let sub = Subscription {
        tickers: ticker_list,
        sender: quote_sender,
    };
    subscription_sender
        .send(sub)
        .map_err(|_| "Subscription receiver завершен")?;

    let join = std::thread::spawn(move || -> Result<(), String> {
        udp_thread(&udp_addr, udp, quote_receiver, timeout, run)
    });

    debug!("Запускаем UDP");

    return Ok(Command {
        response: "OK\n".to_string(),
        join: Some(join),
    });
}

fn connection_handler(
    stream: TcpStream,
    subscription_sender: Sender<Subscription>,
    timeout: Duration,
    run: Arc<AtomicBool>,
) -> Result<(), String> {
    let peer_addr = stream
        .peer_addr()
        .map_err(|e| format!("Ошибка при определении адреса клиента {}", e))?;

    debug!("Новое соединение {}", peer_addr);

    stream
        .set_read_timeout(Some(Duration::from_secs(1)))
        .map_err(|e| format!("Ошибка при вызове set_read_timeout {}", e))?;

    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    loop {
        if run.load(Ordering::Relaxed) == false {
            debug!("Получен Ctrl+C до получения команды от {}", peer_addr);
            let _ = stream.shutdown(std::net::Shutdown::Both);
            return Ok(());
        }

        let mut buf = String::new();

        let receive_result = reader.read_line(&mut buf);
        match receive_result {
            Ok(0) => {
                debug!("Соединение c {} закрыто до получения команды", peer_addr);
                return Ok(());
            }
            Ok(x) => {
                debug!("От {} получена команда {} байт", &peer_addr, x);
                trace!("{}", buf);
                let Command { response, join } =
                    process_command(&buf, subscription_sender, timeout, run)?;

                debug!("Посылаем {} ответ {} байт", &peer_addr, response.len());
                trace!("{}", response);
                let _ = writer.write(response.as_bytes()).map_err(|_| {
                    format!("Соединение c {} закрыто до отправки ответа", &peer_addr)
                })?;
                writer
                    .flush()
                    .map_err(|_| format!("Не могу сделать flush для {}", &peer_addr))?;

                stream
                    .shutdown(std::net::Shutdown::Both)
                    .map_err(|_| format!("Не могу сделать shutodwn для {}", &peer_addr))?;

                if let Some(join) = join {
                    match join.join() {
                        Ok(x) => x?,
                        Err(_) => return Err("UDP JoinError".to_string()),
                    }
                }
                debug!("Поток для {} завершен успешно", &peer_addr);
                return Ok(());
            }
            Err(err) => match err.kind() {
                std::io::ErrorKind::WouldBlock => {}
                _ => {
                    warn!("Ошибка при вызове read_to_string {}", err.to_string());
                    return Ok(());
                }
            },
        }
    }
}

pub fn listen(
    subscription_sender: Sender<Subscription>,
    args: Args,
    run: Arc<AtomicBool>,
) -> Result<(), NetworkError> {
    let listener = TcpListener::bind(SocketAddr::from(([0; 4], args.port)))
        .map_err(|_| NetworkError::ListenError)?;
    listener
        .set_nonblocking(true)
        .map_err(|_| NetworkError::SetNonblocking)?;

    let mut threads: Vec<_> = Vec::new();
    info!("Прослушиватель запущен. Прослушиваемый порт: {}", args.port);

    loop {
        if run.load(Ordering::Relaxed) == false {
            break;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                let subscription_sender = subscription_sender.clone();
                let timeout = args.timeout;
                let run = Arc::clone(&run);
                let thread = std::thread::spawn(move || {
                    connection_handler(
                        stream,
                        subscription_sender,
                        Duration::from_secs(timeout),
                        run,
                    )
                });
                threads.push(Some(thread));
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                }
                _ => return Err(NetworkError::AcceptError),
            },
        }

        for i in 0..threads.len() {
            if let Some(thread) = &threads[i] {
                if thread.is_finished() {
                    if let Some(thread) = threads[i].take() {
                        if let Ok(Err(err)) = thread.join() {
                            debug!("{}", err);
                        }
                    }
                }
            }
        }

        threads.retain(|thread| thread.is_some());
    }

    for thread in threads.drain(..) {
        if let Some(thread) = thread {
            if let Ok(Err(err)) = thread.join() {
                debug!("{}", err);
            }
        }
    }

    info!("Прослушиватель остановлен");
    Ok(())
}
