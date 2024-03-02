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
    // Member::create(&mongodb, &client).await; //ok
    // FinancialYear::create(&mongodb, &client).await; //ok
    // TdsNatureOfPayment::create(&mongodb, &client).await;
    // Account::map(&mongodb).await; // Ok
    // Account::create(&mongodb, &client).await; // Ok
    // GstRegistration::create(&mongodb, &client).await; //ok
    // Branch::create(&mongodb, &client).await; // ok
    //                                          // Contact::create(&mongodb, &client).await;
    //                                          // Doctor::create(&mongodb, &client).await;
    //                                          // Patient::create(&mongodb, &client).await;
    //                                          // Salt::create(&mongodb, &client).await;
    //                                          // Manufacturer::create(&mongodb, &client).await;
    //                                          // Division::create(&mongodb, &client).await;
    //                                          // PosTerminal::create(&mongodb, &client).await;
    // VoucherType::create(&mongodb, &client).await; // ok
    Voucher::create(&mongodb, &client).await;
    // Unit::create(&mongodb, &client).await;
    // Inventory::create(&mongodb, &client).await;
    // PharmaSalt::create(&mongodb, &client).await;
    // SaleIncharge::create(&mongodb, &client).await;
    // PrintTemplate::create(&mongodb, &client).await;
    // DesktopClient::create(&mongodb, &client).await;

    println!("Hello, world!");
}
