use clap::Parser;
use mongodb::Client as MongoClient;
use tokio_postgres::NoTls;

mod model;
use model::*;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// mongodb Organization cluster MONGO-URI.
    #[clap(short, long, default_value = "mongodb://localhost:27017/velavanmed")]
    mongodb: String,

    /// postgres Organization HOST.
    #[clap(
        short,
        long,
        default_value = "postgresql://postgres:1@localhost:5432/velavanmeddemo"
    )]
    postgres: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let (client, connection) = tokio_postgres::connect(&args.postgres, NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let mongodb = MongoClient::with_uri_str(args.mongodb)
        .await
        .unwrap()
        .default_database()
        .unwrap();
    println!("START***{}****", &mongodb.name());
    println!("Member start..");
    Member::create(&mongodb, &client).await; //ok
    println!("FinancialYear start..");
    FinancialYear::create(&mongodb, &client).await; //ok
    println!("TdsNatureOfPayment start..");
    TdsNatureOfPayment::create(&mongodb, &client).await;
    println!("Account map start..");
    Account::map(&mongodb).await; // Ok
    println!("Account start..");
    Account::create(&mongodb, &client).await; // Ok
    println!("GstRegistration start..");
    GstRegistration::create(&mongodb, &client).await; //ok
    println!("Branch start..");
    Branch::create(&mongodb, &client).await; // ok
    println!("Customer Vendor start..");
    Contact::create(&mongodb, &client).await; // ok
    println!("Doctor start..");
    Doctor::create(&mongodb, &client).await; // ok
    println!("Manufacturer start..");
    Manufacturer::create(&mongodb, &client).await; //ok
    println!("Division start..");
    Division::create(&mongodb, &client).await; //ok
    println!("PosTerminal start..");
    PosTerminal::create(&mongodb, &client).await; // ok
    println!("VoucherType start..");
    VoucherType::create(&mongodb, &client).await; // ok
    println!("AccountOpening start..");
    AccountOpening::create(&mongodb, &client).await; // ok
    println!("Voucher start..");
    Voucher::create(&mongodb, &client).await; //ok
    println!("Unit start..");
    Unit::create(&mongodb, &client).await; //ok
    println!("Stock location start..");
    Rack::create(&mongodb, &client).await; //ok
    println!("SaleIncharge start..");
    SaleIncharge::create(&mongodb, &client).await; //ok
    println!("DesktopClient start..");
    // DesktopClient::create(&mongodb, &client).await; //ok
    println!("Salt create start..");
    Salt::create(&mongodb, &client).await; //ok
    println!("InventoryBranchBatch create start..");
    InventoryBranchBatch::create(&mongodb).await; //ok
    println!("Inventory create start..");
    Inventory::create(&mongodb, &client).await; //ok
    println!("InventoryBranchBatch opening start..");
    InventoryBranchBatch::opening(&mongodb, &client).await;
    println!("VoucherNumSequence start..");
    VoucherNumSequence::create(&mongodb, &client).await;
    println!("END***{}****", &mongodb.name());
}
