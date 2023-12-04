use std::str::FromStr;

use futures_util::{StreamExt, TryStreamExt};
use mongodb::{
    bson::{doc, Document},
    options::{FindOptions, UpdateOptions},
    Database,
};
use regex::Regex;
// use tokio_postgres::types::Type;
use chrono::NaiveDate;
use tokio_postgres::Client as PostgresClient;
// use uuid::Uuid;
pub const VOUCHER_COLLECTION: [&str; 8] = [
    "paymemts",
    "contras",
    "receipts",
    "journals",
    "purchases",
    "credit_notes",
    "debit_notes",
    "sales",
];

mod account;
mod financial_year;
mod member;
mod rack;
mod tds_nature_of_payment;

pub use account::Account;
pub use financial_year::FinancialYear;
pub use member::Member;
pub use rack::Rack;
pub use tds_nature_of_payment::TdsNatureOfPayment;

pub trait Doc {
    fn get_string(&self, key: &str) -> Option<String>;
    fn _get_document(&self, key: &str) -> Option<Document>;
    fn _get_f64(&self, key: &str) -> Option<f64>;
    fn get_array_document(&self, key: &str) -> Option<Vec<Document>>;
}

impl Doc for Document {
    fn get_string(&self, key: &str) -> Option<String> {
        self.get_str(key).map(|x| x.to_string()).ok()
    }
    fn _get_document(&self, key: &str) -> Option<Document> {
        self.get_document(key).ok().cloned()
    }
    fn get_array_document(&self, key: &str) -> Option<Vec<Document>> {
        self.get_array(key)
            .map(|x| {
                x.iter()
                    .map(|x| x.as_document().unwrap().clone())
                    .collect::<Vec<Document>>()
            })
            .ok()
    }

    fn _get_f64(&self, key: &str) -> Option<f64> {
        if let Ok(f) = self.get_f64(key) {
            return Some(f);
        } else if let Ok(i) = self.get_i64(key) {
            return Some(i as f64);
        } else if let Ok(i) = self.get_i32(key) {
            return Some(i as f64);
        }
        None
    }
}

fn val_name(name: &str) -> String {
    let re = Regex::new("[^a-zA-Z\\d]").unwrap();
    re.replace_all(name, "").to_lowercase()
}
fn find_opts(projection: Document, sort: Document) -> FindOptions {
    FindOptions::builder()
        .projection(projection)
        .sort(sort)
        .build()
}

// fn oid_uuid(oid: ObjectId) -> Uuid {
//     let var = oid.to_hex().replace("-", "");
//     let re = Regex::new("/[x]/"g).unwrap();
//     let x = "xxxxxxxx-xxxx-4xxx-xxxx-xxxxxxxxxxxx"
//         .to_string()
//         .replace(re.to_string(), |c: &str, p: u8| var[p % var.len() as u8]);

//     Uuid::parse_str("647883eb-84d6-4724-25ec-647883eb484d").unwrap()
// }
