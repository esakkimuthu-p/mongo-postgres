use futures_util::{StreamExt, TryStreamExt};
use mongodb::{
    bson::{doc, Document},
    options::FindOptions,
    Database,
};
use tokio_postgres::Client as PostgresClient;

mod account;
mod account_opening;
mod branch;
mod closing_batch;
mod contact;
// mod desktop_client;
mod division;
mod doctor;
mod financial_year;
mod gst_registration;
mod inventory;
mod manufacturer;
mod member;
mod pos_terminal;
mod rack;
mod sale_incharge;
mod salt;
mod section;
mod tds_nature_of_payment;
mod unit;
mod vendor_item_map;
mod voucher;
mod voucher_num_seq;
mod voucher_type;

pub use account::Account;
pub use account_opening::AccountOpening;
pub use branch::Branch;
pub use closing_batch::InventoryBranchBatch;
pub use contact::Contact;
pub use section::Section;
pub use vendor_item_map::VendorBillItem;
// pub use desktop_client::DesktopClient;
pub use division::Division;
pub use doctor::Doctor;
pub use financial_year::FinancialYear;
pub use gst_registration::GstRegistration;
pub use inventory::Inventory;
pub use manufacturer::Manufacturer;
pub use member::Member;
pub use pos_terminal::PosTerminal;
pub use rack::Rack;
pub use sale_incharge::SaleIncharge;
pub use salt::Salt;
pub use tds_nature_of_payment::TdsNatureOfPayment;
pub use unit::Unit;
pub use voucher::Voucher;
pub use voucher_num_seq::VoucherNumSequence;
pub use voucher_type::VoucherType;

pub fn round64(number: f64, precision: u8) -> f64 {
    let scale = 10_f64.powi(precision as i32);
    (number * scale).round() / scale
}

pub trait Doc {
    fn get_string(&self, key: &str) -> Option<String>;
    fn _get_document(&self, key: &str) -> Option<Document>;
    fn _get_f64(&self, key: &str) -> Option<f64>;
    fn _get_i32(&self, key: &str) -> Option<i32>;
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
    fn _get_i32(&self, key: &str) -> Option<i32> {
        if let Ok(f) = self.get_i32(key) {
            return Some(f);
        } else if let Ok(i) = self.get_i64(key) {
            return Some(i as i32);
        } else if let Ok(i) = self.get_f64(key) {
            return Some(i as i32);
        }
        None
    }
}

fn find_opts(projection: Document, sort: Document) -> FindOptions {
    if sort.is_empty() {
        FindOptions::builder().projection(projection).build()
    } else {
        FindOptions::builder()
            .projection(projection)
            .sort(sort)
            .build()
    }
}

// fn oid_uuid(oid: ObjectId) -> Uuid {
//     let var = oid.to_hex().replace("-", "");
//     let re = Regex::new("/[x]/"g).unwrap();
//     let x = "xxxxxxxx-xxxx-4xxx-xxxx-xxxxxxxxxxxx"
//         .to_string()
//         .replace(re.to_string(), |c: &str, p: u8| var[p % var.len() as u8]);

//     Uuid::parse_str("647883eb-84d6-4724-25ec-647883eb484d").unwrap()
// }
