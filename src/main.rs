use clap::Parser;
use mongodb::Client as MongoClient;
use tokio_postgres::NoTls;

mod model;
use model::*;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// mongodb Organization cluster MONGO-URI.
    #[clap(short, long, default_value = "mongodb://localhost:27017/ttgoldpalace")]
    mongodb: String,

    /// postgres Organization HOST.
    #[clap(
        short,
        long,
        default_value = "postgresql://postgres:1@localhost:55432/rkmedicals"
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
    Member::create(&mongodb, &client).await;
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
    println!("insert into ac_txn for inv_opening..");
    client
        .execute("insert into ac_txn (id, sno, date, eff_date, account_id, account_name, base_account_types, branch_id, branch_name, debit, is_opening)
(select gen_random_uuid(),1, min(date), min(date), 16,'Inventory Asset', array ['STOCK'], a.branch_id,min(a.branch_name), round(sum(asset_amount)::numeric, 2)::float, true from inv_txn a group by a.branch_id);", &[])
        .await
        .unwrap();
    println!("refresh materialized start..");
    client
        .execute("refresh materialized view mvw_account_daily_summary", &[])
        .await
        .unwrap();
    println!("END***{}****", &mongodb.name());
}
