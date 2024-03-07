use std::collections::HashMap;

use serde_json::{json, Value};
use uuid::Uuid;

use super::*;

pub struct Voucher;

pub const VOUCHER_COLLECTION: [&str; 8] = [
    "payments",
    "contras",
    "receipts",
    "journals",
    "purchases",
    "credit_notes",
    "debit_notes",
    "sales",
];

impl Voucher {
    fn voucher_no(v_no: &str, branch_prefix: &str, fy: &str) -> (String, i32, i32) {
        let mut alpha = v_no.split(char::is_numeric).collect::<String>();
        let numeric = v_no.split(char::is_alphabetic).collect::<String>();
        let mut seq = numeric.clone().split(fy).collect::<String>();
        if seq.is_empty() {
            seq = numeric;
        }
        if alpha.is_empty() {
            alpha = branch_prefix.to_string();
        }
        (
            alpha.clone(),
            fy.parse::<i32>().unwrap(),
            seq.parse::<i32>().unwrap(),
        )
    }
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let gst_taxes = HashMap::from([
            ("gstna", 0.0),
            ("gstexempt", 0.0),
            ("gstngs", 0.0),
            ("gst0", 0.0),
            ("gst0p1", 0.1),
            ("gst0p25", 0.25),
            ("gst1", 1.0),
            ("gst1p5", 1.5),
            ("gst3", 3.0),
            ("gst5", 5.0),
            ("gst7p5", 7.5),
            ("gst12", 12.0),
            ("gst18", 18.0),
            ("gst28", 28.0),
        ]);
        let branches = mongodb
            .collection::<Document>("branches")
            .find(
                doc! {},
                find_opts(
                    doc! {"_id": 1, "postgres": 1, "name": 1, "voucherNoPrefix": 1},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let financial_years = mongodb
            .collection::<Document>("financial_years")
            .find(doc! {}, None)
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let voucher_types = mongodb
            .collection::<Document>("voucher_types")
            .find(
                doc! {},
                find_opts(
                    doc! {"_id": 1, "postgres": 1, "voucherType": 1},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let accounts = mongodb
            .collection::<Document>("accounts")
            .find(
                doc! {},
                find_opts(doc! {"_id": 1, "postgres": 1}, doc! {"_id": 1}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();

        for collection in VOUCHER_COLLECTION {
            println!("start {}...", collection);
            let mut cur = mongodb
            .collection::<Document>(collection)
            .find(
                doc! {},
                find_opts(
                    doc! {"createdBy": 0, "createdAt": 0, "updatedAt": 0, "updatedBy": 0, "invTrns": 0},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap();
            while let Some(Ok(d)) = cur.next().await {
                let mut gst_location_type = None;
                let mut party_gst = None;
                let mut branch_gst = None;
                if let Some(br_gst) = d._get_document("branchGst") {
                    if !br_gst.is_empty() {
                        gst_location_type = Some("LOCAL");
                        branch_gst = Some(json!({
                            "reg_type": br_gst.get_str("regType").unwrap(), 
                            "location": br_gst.get_str("location").unwrap(),
                            "gst_no": br_gst.get_str("gstNo").unwrap()}));
                    }
                    if let Some(gst) = d._get_document("partyGst") {
                        if !gst.is_empty() {
                            if gst.get_str("location").unwrap_or_default()
                                != br_gst.get_str("location").unwrap()
                            {
                                gst_location_type = Some("INTER_STATE");
                            }
                            let reg_type = if gst.get_str("regType").unwrap() == "CONSUMER" {
                                "UNREGISTERED"
                            } else if gst.get_str("regType").unwrap() == "OVERSEAS" {
                                "IMPORT_EXPORT"
                            } else {
                                gst.get_str("regType").unwrap()
                            };
                            let mut p_gst = json!({
                                "reg_type": reg_type, 
                                "location": gst.get_str("location").ok()});
                            if let Ok(x) = gst.get_str("location") {
                                p_gst["gst_no"] = json!(x);
                            }
                            party_gst = Some(p_gst);
                        }
                    }
                }

                let (branch, branch_name, branch_prefix) = branches
                    .iter()
                    .find_map(|x| {
                        (x.get_object_id("_id").unwrap() == d.get_object_id("branch").unwrap())
                            .then_some((
                                x.get_i32("postgres").unwrap(),
                                x.get_str("name").unwrap(),
                                x.get_str("voucherNoPrefix").unwrap(),
                            ))
                    })
                    .unwrap();
                let fy = financial_years
                    .iter()
                    .find(|x| {
                        x.get_string("fStart").unwrap() <= d.get_string("date").unwrap()
                            && x.get_string("fEnd").unwrap() >= d.get_string("date").unwrap()
                    })
                    .unwrap();
                let fy = format!(
                    "{}{}",
                    &fy.get_string("fStart").unwrap()[2..=3],
                    &fy.get_string("fEnd").unwrap()[2..=3]
                );
                let (voucher_type, base_voucher_type) = voucher_types
                    .iter()
                    .find_map(|x| {
                        (x.get_object_id("_id").unwrap()
                            == d.get_object_id("voucherTypeId").unwrap())
                        .then_some((
                            x.get_i32("postgres").unwrap(),
                            x.get_str("voucherType").unwrap(),
                        ))
                    })
                    .unwrap();
                let voucher_no =
                    Self::voucher_no(&d.get_string("voucherNo").unwrap(), branch_prefix, &fy);
                let mut ac_trns: Vec<Value> = Vec::new();
                let trns = d.get_array_document("acTrns").unwrap_or_default();
                let mut amount = 0.0;
                for trn in trns.clone() {
                    if trn.get_str("accountType").unwrap() != "STOCK" {
                        amount += trn._get_f64("debit").unwrap();
                    }
                    let mut ba: Vec<Value> = Vec::new();
                    let mut bk: Vec<Value> = Vec::new();
                    if [
                        "TRADE_PAYABLE",
                        "TRADE_RECEIVABLE",
                        "ACCOUNT_PAYABLE",
                        "ACCOUNT_RECEIVABLE",
                    ]
                    .contains(&trn.get_str("accountType").unwrap())
                    {
                        ba.push(json!({
                            "id": Uuid::new_v4(),
                            "amount": trn._get_f64("debit").unwrap() - trn._get_f64("credit").unwrap(),
                            "ref_type": "ON_ACC",
                            "ref_no": d.get_string("refNo"),
                        }));
                    }
                    if ["BANK_ACCOUNT", "BANK_OD_ACCOUNT"]
                        .contains(&trn.get_str("accountType").unwrap())
                    {
                        let sundry_account = &trns.iter().find_map(|x| {
                            ([
                                "TRADE_PAYABLE",
                                "TRADE_RECEIVABLE",
                                "ACCOUNT_PAYABLE",
                                "ACCOUNT_RECEIVABLE",
                            ]
                            .contains(&x.get_str("accountType").unwrap()))
                            .then_some(x.get_object_id("account").unwrap())
                        });
                        let except_bk_account = &trns
                            .iter()
                            .find_map(|x| {
                                (!["BANK_ACCOUNT", "BANK_OD_ACCOUNT"]
                                    .contains(&x.get_str("accountType").unwrap()))
                                .then_some(x.get_object_id("account").unwrap())
                            })
                            .unwrap();
                        let account = sundry_account.unwrap_or(*except_bk_account);
                        let account = accounts
                            .iter()
                            .find_map(|x| {
                                (x.get_object_id("_id").unwrap() == account)
                                    .then_some(x.get_i32("postgres").unwrap())
                            })
                            .unwrap();
                        bk.push(json!({
                        "id": Uuid::new_v4(),
                        "amount": trn._get_f64("debit").unwrap() - trn._get_f64("credit").unwrap(),
                        "txn_type": "CASH",
                        "account": account,
                    }));
                    }
                    let account = accounts
                        .iter()
                        .find_map(|x| {
                            (x.get_object_id("_id").unwrap()
                                == trn.get_object_id("account").unwrap())
                            .then_some(x.get_i32("postgres").unwrap())
                        })
                        .unwrap();
                    let mut ac_trn = json!({
                        "id": Uuid::new_v4(),
                        "account": account,
                        "debit": trn._get_f64("debit").unwrap(),
                        "credit": trn._get_f64("credit").unwrap(),
                    });
                    if !ba.is_empty() {
                        ac_trn["bill_allocations"] = json!(ba);
                    }
                    if !bk.is_empty() {
                        ac_trn["bank_allocations"] = json!(bk);
                    }
                    if let Ok(x) = trn.get_bool("isDefault") {
                        ac_trn["is_default"] = json!(x);
                    }
                    if let (Some(x), true) = (
                        trn.get_string("tax"),
                        (!["GST_RECEIVABLE", "GST_PAYABLE"]
                            .contains(&trn.get_str("accountType").unwrap())),
                    ) {
                        ac_trn["gst_tax"] = json!(x);
                        ac_trn["qty"] = json!(1.0);
                        let gst_tax = gst_taxes
                            .clone()
                            .into_iter()
                            .find_map(|y| (y.0 == x).then_some(y.1))
                            .unwrap();
                        let taxable =
                            trn._get_f64("debit").unwrap() + trn._get_f64("credit").unwrap();
                        if gst_location_type.unwrap_or_default() == "LOCAL" {
                            ac_trn["sgst_amount"] =
                                json!(round64(taxable * ((gst_tax / 2.0) / 100.00), 2));
                            ac_trn["cgst_amount"] =
                                json!(round64(taxable * ((gst_tax / 2.0) / 100.00), 2));
                        } else {
                            ac_trn["igst_amount"] = json!(round64(taxable * (gst_tax / 100.00), 2));
                        }
                        ac_trn["taxable_amount"] = json!(taxable);
                    }
                    ac_trns.push(ac_trn);
                }
                let desc = format!(
                    "{} OLD-NO: {}",
                    d.get_str("description").unwrap_or_default(),
                    d.get_string("voucherNo").unwrap()
                );
                postgres
                .execute(
                    "INSERT INTO vouchers (
                        date,
                        eff_date,
                        branch,
                        voucher_type,
                        mode,
                        ref_no,
                        description,
                        ac_trns,
                        amount,
                        lut,
                        rcm,
                        branch_name,
                        base_voucher_type,
                        voucher_prefix,
                        voucher_fy,
                        voucher_seq,
                        voucher_no,
                        branch_gst,
                        party_gst,
                        gst_location_type
                    ) VALUES 
                    ($1::TEXT::DATE, $2::TEXT::DATE, $3, $4, $5::TEXT::typ_voucher_mode, $6, $7, 
                        $8::JSONB, $9, $10, $11, $12, $13::TEXT::typ_base_voucher_type,$14,$15,$16,$17,
                        $18::JSON, $19::JSON, $20::TEXT::typ_gst_location_type)",
                    &[
                        &d.get_string("date").unwrap(),
                        &d.get_string("effDate"),
                        &branch,
                        &voucher_type,
                        &"ACC",
                        &d.get_string("refNo"),
                        &desc,
                        &serde_json::to_value(ac_trns).unwrap(),
                        &amount,
                        &d.get_bool("lut").unwrap_or_default(),
                        &d.get_bool("rcm").unwrap_or_default(),
                        &branch_name,
                        &base_voucher_type,
                        &voucher_no.0,
                        &voucher_no.1,
                        &voucher_no.2,
                        &d.get_string("voucherNo").unwrap(),
                        &branch_gst,
                        &party_gst,
                        &gst_location_type
                    ],
                )
                .await
                .unwrap();
            }
            println!("end {}...", collection);
        }
    }
}
