use async_std::prelude::*;
use async_std::stream;
use chrono::prelude::*;
use clap::Clap;
use futures::future;
use std::io::{Error, ErrorKind};
use std::time::Duration;
use yahoo_finance_api as yahoo;
use async_trait::async_trait;
use xactor::*;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;

#[derive(Clap)]
#[clap(
version = "1.0",
author = "Claus Matzinger",
about = "A Manning LiveProject: async Rust"
)]
struct Opts {
    #[clap(short, long, default_value = "AAPL,MSFT,UBER,GOOG")]
    symbols: String,
    #[clap(short, long)]
    from: String,
}

///
/// A trait to provide a common interface for all signal calculations.
///
#[async_trait]
trait StockSignal {
    ///
    /// The signal's data type.
    ///
    type SignalType;

    ///
    /// Calculate the signal on the provided series.
    ///
    /// # Returns
    ///
    /// The signal (using the provided type) or `None` on error/invalid data.
    ///
    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType>;
}

///
/// Calculates the absolute and relative difference between the beginning and ending of an f64 series.
/// The relative difference is relative to the beginning.
///
struct PriceDifference {}

#[async_trait]
impl StockSignal for PriceDifference {
    ///
    /// A tuple `(absolute, relative)` to represent a price difference.
    ///
    type SignalType = (f64, f64);

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if !series.is_empty() {
            // unwrap is safe here even if first == last
            let (first, last) = (series.first().unwrap(), series.last().unwrap());
            let abs_diff = last - first;
            let first = if *first == 0.0 { 1.0 } else { *first };
            let rel_diff = abs_diff / first;
            Some((abs_diff, rel_diff))
        } else {
            None
        }
    }
}

///
/// Window function to create a simple moving average
///
struct WindowedSMA {
    pub window_size: usize,
}

#[async_trait]
impl StockSignal for WindowedSMA {
    type SignalType = Vec<f64>;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if !series.is_empty() && self.window_size > 1 {
            Some(
                series
                    .windows(self.window_size)
                    .map(|w| w.iter().sum::<f64>() / w.len() as f64)
                    .collect(),
            )
        } else {
            None
        }
    }
}

///
/// Find the maximum in a series of f64
///
struct MaxPrice {}

#[async_trait]
impl StockSignal for MaxPrice {
    type SignalType = f64;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            None
        } else {
            Some(series.iter().fold(f64::MIN, |acc, q| acc.max(*q)))
        }
    }
}

///
/// Find the maximum in a series of f64
///
struct MinPrice {}

#[async_trait]
impl StockSignal for MinPrice {
    type SignalType = f64;

    async fn calculate(&self, series: &[f64]) -> Option<Self::SignalType> {
        if series.is_empty() {
            None
        } else {
            Some(series.iter().fold(f64::MAX, |acc, q| acc.min(*q)))
        }
    }
}

///
/// Retrieve data from a data source and extract the closing prices. Errors during download are mapped onto io::Errors as InvalidData.
///
async fn fetch_closing_data(
    symbol: &str,
    beginning: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> std::io::Result<Vec<f64>> {
    let provider = yahoo::YahooConnector::new();
    let response = provider
        .get_quote_history(symbol, *beginning, *end)
        .await
        .map_err(|_| Error::from(ErrorKind::InvalidData))?;
    let mut quotes = response
        .quotes()
        .map_err(|_| Error::from(ErrorKind::InvalidData))?;
    if !quotes.is_empty() {
        quotes.sort_by_cached_key(|k| k.timestamp);
        Ok(quotes.iter().map(|q| q.adjclose as f64).collect())
    } else {
        Ok(vec![])
    }
}

// Downloading Actor

#[message]
#[derive(Clone)]
struct SymbolMsg {
    symbol: String,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

#[derive(Default)]
struct DownloadingActor;

#[async_trait::async_trait]
impl Actor for DownloadingActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        ctx.subscribe::<SymbolMsg>().await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<SymbolMsg> for DownloadingActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: SymbolMsg) {
        let symbol = &msg.symbol;
        let beginning = &msg.from;
        let end = &msg.to;
        match fetch_closing_data(symbol, beginning, end).await {
            Ok(closes) => {
                if let Err(e) = Broker::from_registry().await.unwrap().publish(ClosesMsg {
                    symbol: symbol.clone(),
                    beginning: msg.from,
                    closes,
                }) {
                    eprintln!("no ClosesMsg broker {}", e);
                }
            }
            Err(e) => {
                eprintln!("Error fetching '{}': {}", symbol, e);
            }
        }
    }
}

// ProcessingActor

#[message]
#[derive(Clone)]
struct ClosesMsg {
    symbol: String,
    beginning: DateTime<Utc>,
    closes: Vec<f64>,
}

#[derive(Default)]
struct ProcessingActor;

#[async_trait::async_trait]
impl Actor for ProcessingActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        ctx.subscribe::<ClosesMsg>().await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<ClosesMsg> for ProcessingActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: ClosesMsg) {
        let closes = msg.closes;
        if !closes.is_empty() {
            let diff = PriceDifference {};
            let min = MinPrice {};
            let max = MaxPrice {};
            let sma = WindowedSMA { window_size: 30 };

            let period_max: f64 = max.calculate(&closes).await.unwrap_or(-1.0);
            let period_min: f64 = min.calculate(&closes).await.unwrap_or(-1.0);

            let last_price = *closes.last().unwrap_or(&-1.0);
            let (_, pct_change) = diff.calculate(&closes).await.unwrap_or((-1.0, -1.0));
            let sma = sma.calculate(&closes).await.unwrap_or(vec![]);

            if let Err(e) = Broker::from_registry().await.unwrap().publish(StatsMsg {
                symbol: msg.symbol,
                beginning: msg.beginning,
                last_price,
                pct_change: pct_change * 100.0,
                period_min,
                period_max,
                sma_last: *sma.last().unwrap_or(&0.0),
            }) {
                eprintln!("no StatsMsg broker {}", e);
            }
        }
    }
}

// PrintingActor and WritingActor

#[message]
#[derive(Clone)]
struct StatsMsg {
    pub symbol: String,
    pub beginning: DateTime<Utc>,
    pub last_price: f64,
    pub pct_change: f64,
    pub period_min: f64,
    pub period_max: f64,
    pub sma_last: f64,
}

#[derive(Default)]
struct PrintingActor;

#[async_trait::async_trait]
impl Actor for PrintingActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        ctx.subscribe::<StatsMsg>().await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<StatsMsg> for PrintingActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: StatsMsg) {
        // a simple way to output CSV data
        println!(
            "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}",
            msg.beginning.to_rfc3339(),
            msg.symbol,
            msg.last_price,
            msg.pct_change,
            msg.period_min,
            msg.period_max,
            msg.sma_last
        );
    }
}

#[derive(Default)]
struct WritingActor {
    filename: String,
    writer: Option<BufWriter<File>>,
}

#[async_trait::async_trait]
impl Actor for WritingActor {
    async fn started(&mut self, ctx: &mut Context<Self>) -> Result<()> {
        ctx.subscribe::<StatsMsg>().await;
        let mut file = File::create(&self.filename)
            .unwrap_or_else(|_| panic!("Could not open target file '{}'", self.filename));
        self.writer = Some(BufWriter::new(file));
        Ok(())
    }
}

#[async_trait::async_trait]
impl Handler<StatsMsg> for WritingActor {
    async fn handle(&mut self, _ctx: &mut Context<Self>, msg: StatsMsg) {
        // a simple way to output CSV data
        if let Some(buf) = &mut self.writer {
            writeln!(
                buf,
                "{},{},${:.2},{:.2}%,${:.2},${:.2},${:.2}",
                msg.beginning.to_rfc3339(),
                msg.symbol,
                msg.last_price,
                msg.pct_change,
                msg.period_min,
                msg.period_max,
                msg.sma_last
            );
            buf.flush();
        }
    }
}

impl WritingActor {
    fn flush(self: Self) {
        if let Some(mut buf) = self.writer {
            buf.flush();
        }
    }
}

#[xactor::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();
    let from: DateTime<Utc> = opts.from.parse().expect("Couldn't parse 'from' date");
    let symbols: Vec<String> = opts.symbols.split(',').map(|s| s.to_string()).collect();

    let mut interval = stream::interval(Duration::from_secs(5));

    let _downloading_actor_addr = DownloadingActor::start_default().await?;
    let _processing_printing_actor_addr = ProcessingActor::start_default().await?;
    let _printing_actor_addr = PrintingActor::start_default().await?;

    let _printing2_actor_addr = WritingActor { filename: "output.csv".to_string(), writer: None }.start().await?;

    // a simple way to output a CSV header
    println!("period start,symbol,price,change %,min,max,30d avg");
    while let Some(_) = interval.next().await {
        let to = Utc::now();
        for symbol in &symbols {
            if let Err(e) = Broker::from_registry().await.unwrap().publish(SymbolMsg {
                symbol: symbol.clone(),
                from,
                to,
            }) {
                eprintln!("no SymbolMsg broker {}", e);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]

    use super::*;

    #[async_std::test]
    async fn test_PriceDifference_calculate() {
        let signal = PriceDifference {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some((0.0, 0.0)));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some((-1.0, -1.0)));
        assert_eq!(
            signal.calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0]).await,
            Some((8.0, 4.0))
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some((1.0, 1.0))
        );
    }

    #[async_std::test]
    async fn test_MinPrice_calculate() {
        let signal = MinPrice {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some(0.0));
        assert_eq!(
            signal.calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0]).await,
            Some(1.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some(0.0)
        );
    }

    #[async_std::test]
    async fn test_MaxPrice_calculate() {
        let signal = MaxPrice {};
        assert_eq!(signal.calculate(&[]).await, None);
        assert_eq!(signal.calculate(&[1.0]).await, Some(1.0));
        assert_eq!(signal.calculate(&[1.0, 0.0]).await, Some(1.0));
        assert_eq!(
            signal.calculate(&[2.0, 3.0, 5.0, 6.0, 1.0, 2.0, 10.0]).await,
            Some(10.0)
        );
        assert_eq!(
            signal.calculate(&[0.0, 3.0, 5.0, 6.0, 1.0, 2.0, 1.0]).await,
            Some(6.0)
        );
    }

    #[async_std::test]
    async fn test_WindowedSMA_calculate() {
        let series = vec![2.0, 4.5, 5.3, 6.5, 4.7];

        let signal = WindowedSMA { window_size: 3 };
        assert_eq!(
            signal.calculate(&series).await,
            Some(vec![3.9333333333333336, 5.433333333333334, 5.5])
        );

        let signal = WindowedSMA { window_size: 5 };
        assert_eq!(signal.calculate(&series).await, Some(vec![4.6]));

        let signal = WindowedSMA { window_size: 10 };
        assert_eq!(signal.calculate(&series).await, Some(vec![]));
    }
}
