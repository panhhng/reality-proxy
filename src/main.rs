use std::{
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context;
use clap::{Parser, Subcommand};
use rand::{rngs::StdRng, Rng, SeedableRng};
use tokio::{
    io::{split, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::sleep,
};

#[derive(Parser)]
#[command(name = "reality", version, about = "Simulate hostile TCP network conditions")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a TCP proxy between a local address and an upstream server.
    Run(RunArgs),
}

#[derive(clap::Args)]
struct RunArgs {
    /// Local address the application should connect to.
    #[arg(long, default_value = "127.0.0.1:9000")]
    listen: SocketAddr,

    /// Address of the real upstream server.
    #[arg(long)]
    target: SocketAddr,

    /// One-way delay added to each stream before its first bytes are forwarded.
    #[arg(long, default_value = "0ms", value_parser = parse_duration)]
    latency: Duration,

    /// Maximum throughput per direction. Examples: 2mbit, 256KB/s.
    #[arg(long, value_parser = parse_bandwidth)]
    bandwidth: Option<f64>,

    /// Probability of rejecting each new connection, from 0% through 100%.
    #[arg(long, default_value = "0%", value_parser = parse_percentage)]
    drop_rate: f64,

    /// Close each established proxied connection after this duration.
    #[arg(long, value_parser = parse_duration)]
    disconnect_after: Option<Duration>,

    /// Seed for repeatable connection rejection decisions.
    #[arg(long)]
    seed: Option<u64>,
}

struct ProxyConfig {
    target: SocketAddr,
    latency: Duration,
    bandwidth_bytes_per_second: Option<f64>,
    drop_probability: f64,
    disconnect_after: Option<Duration>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run(args) => run(args).await,
    }
}

async fn run(args: RunArgs) -> anyhow::Result<()> {
    let listener = TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("could not listen on {}", args.listen))?;
    let config = Arc::new(ProxyConfig {
        target: args.target,
        latency: args.latency,
        bandwidth_bytes_per_second: args.bandwidth,
        drop_probability: args.drop_rate / 100.0,
        disconnect_after: args.disconnect_after,
    });
    let rng = Arc::new(Mutex::new(match args.seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::from_entropy(),
    }));

    println!("listening on {}; forwarding to {}", args.listen, args.target);
    println!(
        "latency: {:?}, bandwidth: {}, connection drop rate: {:.1}%",
        args.latency,
        args.bandwidth
            .map(|rate| format!("{rate:.0} B/s per direction"))
            .unwrap_or_else(|| "unlimited".to_owned()),
        args.drop_rate
    );

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (client, peer) = accepted?;
                let config = Arc::clone(&config);
                let should_drop = rng
                    .lock()
                    .expect("random number generator mutex poisoned")
                    .gen_bool(config.drop_probability);
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(client, peer, config, should_drop).await {
                        eprintln!("{peer}: {error:#}");
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("failed to listen for Ctrl-C")?;
                println!("shutting down");
                break;
            }
        }
    }
    Ok(())
}

async fn handle_connection(
    mut client: TcpStream,
    peer: SocketAddr,
    config: Arc<ProxyConfig>,
    should_drop: bool,
) -> anyhow::Result<()> {
    if should_drop {
        eprintln!("{peer}: rejected by simulated connection drop");
        return Ok(());
    }

    let upstream = TcpStream::connect(config.target)
        .await
        .with_context(|| format!("could not connect to upstream {}", config.target))?;
    let (client_read, client_write) = split(client);
    let (upstream_read, upstream_write) = split(upstream);
    let transfer = async {
        tokio::try_join!(
            copy_with_reality(
                client_read,
                upstream_write,
                config.latency,
                config.bandwidth_bytes_per_second,
            ),
            copy_with_reality(
                upstream_read,
                client_write,
                config.latency,
                config.bandwidth_bytes_per_second,
            ),
        )?;
        Ok::<(), io::Error>(())
    };

    match config.disconnect_after {
        Some(duration) => {
            tokio::select! {
                result = transfer => result.context("TCP forwarding failed")?,
                _ = sleep(duration) => eprintln!("{peer}: disconnected by simulation timer"),
            }
        }
        None => transfer.await.context("TCP forwarding failed")?,
    }
    Ok(())
}

async fn copy_with_reality<R, W>(
    mut reader: R,
    mut writer: W,
    latency: Duration,
    bandwidth_bytes_per_second: Option<f64>,
) -> io::Result<u64>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    if !latency.is_zero() {
        sleep(latency).await;
    }

    let mut buffer = [0_u8; 16 * 1024];
    let mut forwarded = 0_u64;
    loop {
        let bytes_read = reader.read(&mut buffer).await?;
        if bytes_read == 0 {
            writer.shutdown().await?;
            return Ok(forwarded);
        }
        writer.write_all(&buffer[..bytes_read]).await?;
        forwarded += bytes_read as u64;

        if let Some(rate) = bandwidth_bytes_per_second {
            sleep(Duration::from_secs_f64(bytes_read as f64 / rate)).await;
        }
    }
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    humantime::parse_duration(value).map_err(|error| error.to_string())
}

fn parse_percentage(value: &str) -> Result<f64, String> {
    let number = value.trim_end_matches('%');
    let percentage: f64 = number
        .parse()
        .map_err(|_| "expected a percentage such as 5%".to_owned())?;
    if !percentage.is_finite() || !(0.0..=100.0).contains(&percentage) {
        return Err("percentage must be between 0 and 100".to_owned());
    }
    Ok(percentage)
}

fn parse_bandwidth(value: &str) -> Result<f64, String> {
    let normalized = value.trim().to_ascii_lowercase();
    let (number, multiplier, is_bits) = if let Some(number) = normalized.strip_suffix("mbit") {
        (number, 1_000_000.0, true)
    } else if let Some(number) = normalized.strip_suffix("kbit") {
        (number, 1_000.0, true)
    } else if let Some(number) = normalized.strip_suffix("gbit") {
        (number, 1_000_000_000.0, true)
    } else if let Some(number) = normalized.strip_suffix("mbps") {
        (number, 1_000_000.0, true)
    } else if let Some(number) = normalized.strip_suffix("kbps") {
        (number, 1_000.0, true)
    } else if let Some(number) = normalized.strip_suffix("gbps") {
        (number, 1_000_000_000.0, true)
    } else if let Some(number) = normalized.strip_suffix("mb/s") {
        (number, 1_000_000.0, false)
    } else if let Some(number) = normalized.strip_suffix("kb/s") {
        (number, 1_000.0, false)
    } else if let Some(number) = normalized.strip_suffix("gb/s") {
        (number, 1_000_000_000.0, false)
    } else {
        return Err("use a rate such as 2mbit, 256KB/s, or 100Mbps".to_owned());
    };

    let amount: f64 = number
        .trim()
        .parse()
        .map_err(|_| "bandwidth value must start with a number".to_owned())?;
    let bytes_per_second = amount * multiplier / if is_bits { 8.0 } else { 1.0 };
    if !bytes_per_second.is_finite() || bytes_per_second <= 0.0 {
        return Err("bandwidth must be greater than zero".to_owned());
    }
    Ok(bytes_per_second)
}