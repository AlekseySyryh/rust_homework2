use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, RecvTimeoutError, Sender},
    },
};

use common::StockQuote;
use log::info;

pub struct Subscription {
    pub tickers: Vec<&'static str>,
    pub sender: Sender<Vec<u8>>,
}

pub enum DispatchMessage {
    Quotes(HashMap<&'static str, StockQuote>),
    Subscribe(Subscription),
}

pub fn dispatch(receiver: Receiver<DispatchMessage>, run: Arc<AtomicBool>) {
    let mut subscriptions = Vec::new();
    info!("Диспетчер запущен");
    loop {
        if run.load(Ordering::Relaxed) == false {
            break;
        }
        match receiver.recv_timeout(std::time::Duration::from_secs(1)) {
            Ok(dispatch_message) => match dispatch_message {
                DispatchMessage::Subscribe(subscription) => {
                    subscriptions.push(subscription);
                }
                DispatchMessage::Quotes(quotes) => {
                    subscriptions.retain(|subscription| {
                        for ticker in subscription.tickers.iter() {
                            if let Some(quote) = quotes.get(*ticker) {
                                if let Ok(data) = quote.to_wire_line() {
                                    if let Err(_) = subscription.sender.send(data.into_bytes()) {
                                        return false; // Получателя больще нет - подписка не нужна
                                    }
                                }
                            }
                        }
                        true
                    });
                }
            },
            Err(e) => match e {
                RecvTimeoutError::Timeout => (),         // Нет котировок - Ок
                RecvTimeoutError::Disconnected => break, // Умер producer = стоп
            },
        }
    }
    info!("Диспетчер остановлен");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashMap, sync::mpsc, thread};

    #[test]
    fn it_works() {
        let (dispatcher_sender, dispatch_receiver) = mpsc::channel::<DispatchMessage>();
        let (stock_sender, stock_receiver) = mpsc::channel::<Vec<u8>>();
        let run = Arc::new(AtomicBool::new(true));

        let _ = thread::spawn(move || {
            dispatch(dispatch_receiver, Arc::clone(&run));
        });

        dispatcher_sender
            .send(DispatchMessage::Subscribe(Subscription {
                tickers: vec!["AAPL"],
                sender: stock_sender,
            }))
            .unwrap();
        dispatcher_sender
            .send(DispatchMessage::Quotes(HashMap::from_iter(vec![
                (
                    "AAPL",
                    StockQuote {
                        ticker: "AAPL",
                        ..Default::default()
                    },
                ),
                (
                    "MSFT",
                    StockQuote {
                        ticker: "MSFT",
                        ..Default::default()
                    },
                ),
            ])))
            .unwrap();

        let receive_bytes = stock_receiver.recv().unwrap();
        let receive_string = String::from_utf8(receive_bytes).unwrap();

        let receive = StockQuote::from_wire_line(&receive_string).unwrap();

        assert_eq!(receive.ticker, "AAPL");
    }
}
