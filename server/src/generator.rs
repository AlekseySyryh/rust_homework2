use common::StockQuote;
use log::info;
use rand;
use rand_distr::{Distribution, Normal, Uniform};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MIN_VOLUME: u32 = 0;
const MAX_VOLUME: u32 = 100000;
const MIN_PRICE: f64 = 0.0;
const MAX_PRICE: f64 = 10000.0;
const HIGH_VOLUME_MULTIPLIER: u32 = 2;

struct StockQuotes {
    last_quotes: HashMap<&'static str, StockQuote>,
    high_volume_tickers: HashSet<&'static str>,
    rng: rand::rngs::ThreadRng,
    volume_distibution: Uniform<u32>,
    price_multipler_distibution: Normal<f64>,
}
#[derive(Debug)]
pub enum GeneratorError {
    DistributionError,
    TimeError,
    SendError,
}

impl Display for GeneratorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GeneratorError::DistributionError => write!(f, "Ошибка генерации распределения"),
            GeneratorError::TimeError => write!(f, "Ошибка получения времени"),
            GeneratorError::SendError => write!(f, "Ошибка отправки данных"),
        }
    }
}

impl StockQuotes {
    fn time_now() -> Result<u128, GeneratorError> {
        Ok(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| GeneratorError::TimeError)?
            .as_millis())
    }

    pub fn try_new() -> Result<Self, GeneratorError> {
        let mut quotes: HashMap<&'static str, StockQuote> = HashMap::new();
        let mut rng = rand::rng();

        let volume_distibution = Uniform::new_inclusive(MIN_VOLUME, MAX_VOLUME)
            .map_err(|_| GeneratorError::DistributionError)?;
        let price_multipler_distibution =
            Normal::new(1.0, 0.1).map_err(|_| GeneratorError::DistributionError)?;
        let initial_price_distibution = Uniform::new_inclusive(MIN_PRICE, MAX_PRICE)
            .map_err(|_| GeneratorError::DistributionError)?;

        for ticker in common::VALID_TICKERS.iter() {
            quotes.insert(
                *ticker,
                StockQuote {
                    ticker: *ticker,
                    price: initial_price_distibution.sample(&mut rng),
                    volume: volume_distibution.sample(&mut rng),
                    timestamp_ms: StockQuotes::time_now()?,
                },
            );
        }

        let high_volume_tickers: HashSet<&'static str> = HashSet::from(["AAPL", "MSFT", "TSLA"]);

        Ok(Self {
            last_quotes: quotes,
            volume_distibution: volume_distibution,
            price_multipler_distibution: price_multipler_distibution,
            rng: rng,
            high_volume_tickers: high_volume_tickers,
        })
    }

    pub fn tick(&mut self) -> Result<HashMap<&'static str, StockQuote>, GeneratorError> {
        for quote in self.last_quotes.values_mut() {
            let mut multipler: f64;
            loop {
                multipler = self.price_multipler_distibution.sample(&mut self.rng);
                if multipler > 0.0 {
                    break;
                }
            }

            quote.price = (quote.price * multipler * 100.0).trunc() / 100.0;
            quote.volume = self.volume_distibution.sample(&mut self.rng);
            if self.high_volume_tickers.contains(quote.ticker) {
                quote.volume *= HIGH_VOLUME_MULTIPLIER;
            }
            quote.timestamp_ms = StockQuotes::time_now()?;
        }

        Ok(self.last_quotes.iter().map(|(&k, &v)| (k, v)).collect())
    }
}

pub fn generator(
    sender: Sender<HashMap<&'static str, StockQuote>>,
    sleep: Duration,
    run: Arc<AtomicBool>,
) -> Result<(), GeneratorError> {
    let mut quotes = StockQuotes::try_new()?;
    info!("Генератор запущен");
    loop {
        if run.load(Ordering::Relaxed) == false {
            break;
        }
        let tick_data = quotes.tick()?;
        sender
            .send(tick_data)
            .map_err(|_| GeneratorError::SendError)?;
        thread::sleep(sleep);
    }
    info!("Генератор остановлен");
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::generator::StockQuotes;
    use common::VALID_TICKERS;
    use std::collections::HashSet;

    #[test]
    fn all_tickers_are_available() {
        let mut quotes = StockQuotes::try_new().unwrap();

        let available: HashSet<&str> = quotes.tick().unwrap().into_keys().collect();
        let expected: HashSet<&str> = VALID_TICKERS.iter().copied().collect();

        assert_eq!(available, expected);
    }

    #[test]
    fn some_prices_and_volumes_changes() {
        let mut quotes = StockQuotes::try_new().unwrap();

        let old_quotes = quotes.tick().unwrap();
        let new_quotes = quotes.tick().unwrap();

        let mut prices_are_changes = false;
        let mut volumes_are_changes = false;

        for ticker in VALID_TICKERS.iter() {
            let old = old_quotes.get(*ticker).unwrap();
            let new = new_quotes.get(*ticker).unwrap();

            if !prices_are_changes && old.price != new.price {
                prices_are_changes = true;
            }
            if !volumes_are_changes && old.volume != new.volume {
                volumes_are_changes = true;
            }
            if prices_are_changes && volumes_are_changes {
                break;
            }
        }
        assert!(prices_are_changes, "Некоторые цены должны меняться");
        assert!(volumes_are_changes, "Некоторые объемы должны меняться");
    }
}
