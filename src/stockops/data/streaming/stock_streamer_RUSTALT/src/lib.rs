use pyo3::prelude::*;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use url::Url;

#[pyfunction]
pub fn start_stream(ticker: String, api_token: String) -> PyResult<()> {
    // Spawn a new thread with an async runtime
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let ws_url = format!(
                "wss://ws.eodhistoricaldata.com/ws/us?api_token={}",
                api_token
            );
            let url = Url::parse(&ws_url).unwrap();
            let (mut ws_stream, _) = connect_async(url).await.unwrap();

            let subscribe_msg = format!(
                "{{\"action\":\"subscribe\",\"symbols\":\"{}\"}}",
                ticker
            );
            ws_stream.send(tokio_tungstenite::tungstenite::Message::Text(subscribe_msg)).await.unwrap();

            while let Some(msg) = ws_stream.next().await {
                match msg {
                    Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                        println!("Rust received: {}", text);
                    }
                    _ => {}
                }
            }
        });
    });

    Ok(())
}

#[pymodule]
fn stock_streamer(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(start_stream, m)?)?;
    Ok(())
}
