use std::collections::HashSet;
use std::fmt::Display;
use std::sync::LazyLock;

const TICKER_LIST: &[&'static str] = &[
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "JPM", "JNJ", "V", "PG", "UNH", "HD",
    "DIS", "PYPL", "NFLX", "ADBE", "CRM", "INTC", "CSCO", "PFE", "ABT", "TMO", "ABBV", "LLY",
    "PEP", "COST", "TXN", "AVGO", "ACN", "QCOM", "DHR", "MDT", "NKE", "UPS", "RTX", "HON", "ORCL",
    "LIN", "AMGN", "LOW", "SBUX", "SPGI", "INTU", "ISRG", "T", "BMY", "DE", "PLD", "CI", "CAT",
    "GS", "UNP", "AMT", "AXP", "MS", "BLK", "GE", "SYK", "GILD", "MMM", "MO", "LMT", "FISV", "ADI",
    "BKNG", "C", "SO", "NEE", "ZTS", "TGT", "DUK", "ICE", "BDX", "PNC", "CMCSA", "SCHW", "MDLZ",
    "TJX", "USB", "CL", "EMR", "APD", "COF", "FDX", "AON", "WM", "ECL", "ITW", "VRTX", "D", "NSC",
    "PGR", "ETN", "FIS", "PSA", "KLAC", "MCD", "ADP", "APTV", "AEP", "MCO", "SHW", "DD", "ROP",
    "SLB", "HUM", "BSX", "NOC", "EW",
];

/// Возможные тикеры
pub static VALID_TICKERS: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| TICKER_LIST.iter().copied().collect());

/// Структура для хранения котировки акции
#[derive(Debug, Clone, PartialEq, Default, Copy)]
pub struct StockQuote {
    /// Тикер
    pub ticker: &'static str, //У правильного тикера это ссылка на элемент из TICKER_LIST, что позволит избежать копирований
    /// Цена
    pub price: f64,
    /// Объём
    pub volume: u32,
    /// Время
    pub timestamp_ms: u128,
}

impl Display for StockQuote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Ticker:{}. Price:{}. Volume:{}. Timestamp:{}",
            self.ticker, self.price, self.volume, self.timestamp_ms
        )
    }
}

#[derive(Debug)]
pub enum ParserError {
    WrongNumberOfFields,
    SpaceNotAllowed,
    InvalidTicker,
    InvalidPrice,
    InvalidVolume,
    InvalidTimestamp,
}

impl StockQuote {
    /// Преобразование котировки в строку для отправки по сети
    ///
    /// ```
    /// use common::StockQuote;
    ///
    /// let quote = StockQuote {
    ///     ticker: "AAPL",
    ///     price: 150.0,
    ///     volume: 100000,
    ///     timestamp_ms: 1779121622,
    /// };
    ///
    /// let expected_data = "AAPL|150|100000|1779121622";
    /// let data = quote.to_wire_line().unwrap();
    ///
    /// assert_eq!(data, expected_data);
    /// ```
    pub fn to_wire_line(&self) -> Result<String, ParserError> {
        if !VALID_TICKERS.contains(self.ticker) {
            return Err(ParserError::InvalidTicker);
        }
        Ok(format!(
            "{}|{}|{}|{}",
            self.ticker, self.price, self.volume, self.timestamp_ms
        ))
    }
    /// Получение котировки из строки
    ///
    /// ```
    /// use common::StockQuote;
    ///
    /// let data = "AAPL|150|100000|1779121622";
    ///
    /// let quote = StockQuote::from_wire_line(data).unwrap();
    ///
    /// let expected_quote = StockQuote { ticker: "AAPL", price: 150.0, volume: 100000, timestamp_ms: 1779121622 };
    ///
    /// assert_eq!(quote, expected_quote);
    /// ```
    pub fn from_wire_line(line: &str) -> Result<Self, ParserError> {
        if line.contains(' ') {
            return Err(ParserError::SpaceNotAllowed);
        }
        let lines = line.split('|').collect::<Vec<_>>();
        if lines.len() != 4 {
            return Err(ParserError::WrongNumberOfFields);
        }

        let ticker = VALID_TICKERS
            .get(lines[0])
            .ok_or(ParserError::InvalidTicker)?;

        let price: f64 = lines[1].parse().map_err(|_| ParserError::InvalidPrice)?;
        if price < 0.0 {
            return Err(ParserError::InvalidPrice);
        }

        let volume: u32 = lines[2].parse().map_err(|_| ParserError::InvalidVolume)?;

        let timestamp_ms: u128 = lines[3]
            .parse()
            .map_err(|_| ParserError::InvalidTimestamp)?;

        Ok(StockQuote {
            ticker,
            price,
            volume,
            timestamp_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_ticker_can_not_be_serialized() {
        let quote = StockQuote {
            ticker: "INVALID",
            ..Default::default()
        };

        let data = quote.to_wire_line();

        if !matches!(data, Err(ParserError::InvalidTicker)) {
            panic!("При неверном тикере должна вернуться ошибка InvalidTicker");
        }
    }
    #[test]
    fn serialize_and_deserialize() {
        let quote = StockQuote {
            ticker: "AAPL",
            price: 150.0,
            volume: 100000,
            timestamp_ms: 1779121622,
        };

        let data = quote.to_wire_line().unwrap();

        let deserialized_quote = StockQuote::from_wire_line(&data).unwrap();

        assert_eq!(
            quote, deserialized_quote,
            "Результат декодирования должен совпадать с исходным"
        );
    }

    #[test]
    fn wrong_number_of_fields_not_be_deserialized() {
        let datas = vec![
            "",
            "AAPL",
            "AAPL|150",
            "AAPL|150|100000",
            "AAPL|150|100000|1779121622|123",
        ];

        for data in datas {
            if !matches!(
                StockQuote::from_wire_line(data),
                Err(ParserError::WrongNumberOfFields)
            ) {
                panic!("При неверном числе поле нет должна вернуться ошибка WrongNumberOfFields");
            }
        }
    }

    #[test]
    fn invalid_ticker_can_not_be_deserialized() {
        let datas = vec![
            "|150|100000|1779121622",
            "QSDFD|NotANumber|100000|1779121622",
            "42|NotANumber|100000|1779121622",
        ];

        for data in datas {
            if !matches!(
                StockQuote::from_wire_line(data),
                Err(ParserError::InvalidTicker)
            ) {
                panic!("При неверном тикере должна вернуться ошибка InvalidTicker");
            }
        }
    }

    #[test]
    fn wrong_price_can_not_be_deserialized() {
        let datas = vec![
            "AAPL|-150|100000|1779121622",
            "AAPL|NotANumber|100000|1779121622",
        ];

        for data in datas {
            if !matches!(
                StockQuote::from_wire_line(data),
                Err(ParserError::InvalidPrice)
            ) {
                panic!("При неверной цене должна вернуться ошибка InvalidPrice");
            }
        }
    }

    #[test]
    fn wrong_volume_can_not_be_deserialized() {
        let datas = vec![
            "AAPL|150|-100000|1779121622",
            "AAPL|150|NotANumber|1779121622",
        ];

        for data in datas {
            if !matches!(
                StockQuote::from_wire_line(data),
                Err(ParserError::InvalidVolume)
            ) {
                panic!("При неверном объеме должна вернуться ошибка InvalidVolume");
            }
        }
    }

    #[test]
    fn wrong_timestamp_can_not_be_deserialized() {
        let datas = vec!["AAPL|150|100000|-1779121622", "AAPL|150|100000|NotANumber"];

        for data in datas {
            if !matches!(
                StockQuote::from_wire_line(data),
                Err(ParserError::InvalidTimestamp)
            ) {
                panic!("При неверном времени должна вернуться ошибка InvalidTimestamp");
            }
        }
    }

    #[test]
    fn spaces_in_fields_can_not_be_deserialized() {
        let datas = vec![
            " AAPL|150|100000|1779121622",
            "AAPL |150|100000|1779121622",
            "AAPL| 150|100000|1779121622",
            "AAPL|150 |100000|1779121622",
            "AAPL|150| 100000|1779121622",
            "AAPL|150|100000 |1779121622",
            "AAPL|150|100000| 1779121622",
            "AAPL|150|100000|1779121622 ",
        ];

        for data in datas {
            if !matches!(
                StockQuote::from_wire_line(data),
                Err(ParserError::SpaceNotAllowed)
            ) {
                panic!("При наличии пробелов в полях должна вернуться ошибка SpaceNotAllowed");
            }
        }
    }
}
