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
        default_value = "postgresql://postgres:1@localhost:5432/velavanmedical"
    )]
    postgres: String,
    #[clap(short, long, default_value = "aplus@123$")]
    jwt_private_key: String,
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
    Member::create(&mongodb, &client, &args.jwt_private_key).await;
    println!("FinancialYear start..");
    FinancialYear::create(&mongodb, &client).await;
    println!("TdsNatureOfPayment start..");
    TdsNatureOfPayment::create(&mongodb, &client).await;
    println!("Account map start..");
    Account::map(&mongodb).await;
    println!("Account start..");
    Account::create(&mongodb, &client).await;
    println!("GstRegistration start..");
    GstRegistration::create(&mongodb, &client).await;
    println!("Branch start..");
    Branch::create(&mongodb, &client).await;
    println!("Customer Vendor start..");
    Contact::create(&mongodb, &client).await;
    println!("Doctor start..");
    Doctor::create(&mongodb, &client).await;
    println!("Manufacturer start..");
    Manufacturer::create(&mongodb, &client).await;
    println!("Division start..");
    Division::create(&mongodb, &client).await;
    println!("PosTerminal start..");
    PosTerminal::create(&mongodb, &client).await;
    println!("VoucherType start..");
    VoucherType::create(&mongodb, &client).await;
    println!("AccountOpening start..");
    AccountOpening::create(&mongodb, &client).await;
    println!("Voucher start..");
    Voucher::create(&mongodb, &client).await;
    println!("Unit start..");
    Unit::create(&mongodb, &client).await;
    println!("Stock location start..");
    Rack::create(&mongodb, &client).await;
    println!("Category start..");
    Section::create(&mongodb, &client).await;
    println!("SaleIncharge start..");
    SaleIncharge::create(&mongodb, &client).await;
    println!("DesktopClient start..");
    println!("Salt create start..");
    Salt::create(&mongodb, &client).await;
    println!("InventoryBranchBatch create start..");
    InventoryBranchBatch::create(&mongodb).await;
    println!("Inventory create start..");
    Inventory::create(&mongodb, &client).await;
    println!("InventoryBranchBatch opening start..");
    InventoryBranchBatch::opening(&mongodb, &client).await;
    println!("VoucherNumSequence start..");
    VoucherNumSequence::create(&mongodb, &client).await;
    println!("Vendor create_item_map start..");
    VendorBillItem::create_item_map(&mongodb, &client).await;
    println!("Vendor create_bill_map start..");
    VendorBillItem::create_bill_map(&mongodb, &client).await;
    println!("call day_end_process start..");
    client.execute("call day_end_process()", &[]).await.unwrap();
    println!("END***{}****", &mongodb.name());
}
