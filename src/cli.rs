use clap::Parser;

#[derive(Parser)]
#[command(author, version, about)]
pub struct Args {
    /// Input CSV file path
    #[arg(short, long)]
    pub input: String,

    /// Output CSV file path
    #[arg(short, long)]
    pub output: String,

    /// Minimum electric range filter
    #[arg(short = 'r', long, default_value_t = 200)]
    pub min_range: u16,
    
    /// Skip records with invalid data instead of failing
    #[arg(long, default_value_t = true)]
    pub skip_invalid: bool,

    /// Processing mode: sequential, parallel, batched
    #[arg(short, long, default_value = "sequential")]
    pub mode: String,

    /// Batch size for batched processing
    #[arg(long, default_value_t = 10000)]
    pub batch_size: usize,
}