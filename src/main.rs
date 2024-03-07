use clap::Parser;
// use gql_client::Client;
use mongodb::Client as MongoClient;
use tokio_postgres::NoTls;

mod model;
use model::*;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// mongodb Organization cluster MONGO-URI.
    #[clap(
        short,
        long,
        default_value = "mongodb://testadmin:rootroot@localhost:27017"
    )]
    uri: String,

    /// postgres Organization HOST.
    #[clap(
        short,
        long,
        default_value = "postgresql://postgres:1@localhost:5432/velavanmed"
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

    let mongodb = MongoClient::with_uri_str(args.uri)
        .await
        .unwrap()
        .database("velavanmed1");
    Member::create(&mongodb, &client).await; //ok
    FinancialYear::create(&mongodb, &client).await; //ok
    TdsNatureOfPayment::create(&mongodb, &client).await;
    Account::map(&mongodb).await; // Ok
    Account::create(&mongodb, &client).await; // Ok
    GstRegistration::create(&mongodb, &client).await; //ok
    Branch::create(&mongodb, &client).await; // ok
    Contact::create(&mongodb, &client).await; // ok
    Doctor::create(&mongodb, &client).await; // ok
    Salt::create(&mongodb, &client).await; //ok
    Manufacturer::create(&mongodb, &client).await; //ok
    Division::create(&mongodb, &client).await; //ok
    PosTerminal::create(&mongodb, &client).await; // ok
    VoucherType::create(&mongodb, &client).await; // ok
    AccountOpening::create(&mongodb, &client).await; // ok
    Voucher::create(&mongodb, &client).await; //ok
    Unit::create(&mongodb, &client).await; //ok
    SaleIncharge::create(&mongodb, &client).await; //ok
    DesktopClient::create(&mongodb, &client).await; //ok
    DesktopClient::create(&mongodb, &client).await; //ok
    InventoryBranchBatch::create(&mongodb).await; // ok
    Inventory::create(&mongodb, &client).await;
    // PrintTemplate::create(&mongodb, &client).await; //later

    println!("Hello, world!");
}
