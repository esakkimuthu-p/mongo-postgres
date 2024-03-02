use super::*;
use uuid::Uuid;

pub struct Voucher;

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
        let _collections = [
            "payments",
            "receipts",
            "contras",
            "journals",
            "debit_notes",
            "credit_notes",
            "purchases",
            "sales",
        ];
        for collection in ["debit_notes"] {
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
                let mut ac_trns: Vec<serde_json::Value> = Vec::new();
                let trns = d.get_array_document("acTrns").unwrap_or_default();
                let branch_gst = d._get_document("branchGst").map(|x| {
                    serde_json::json!({
                        "reg_type": x.get_str("regType").unwrap(), 
                        "location": x.get_str("location").unwrap(),
                        "gst_no": x.get_str("gstNo").unwrap()})
                });
                let mut party_gst = None;
                if let Some(gst) = d._get_document("partyGst") {
                    if !gst.is_empty() {
                        party_gst = Some(serde_json::json!({
                            "reg_type": gst.get_str("regType").unwrap(), 
                            "location": gst.get_str("location").ok(),
                            "gst_no": gst.get_str("gstNo").ok()}));
                    }
                }
                let mut amount = 0.0;
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

                for trn in trns.clone() {
                    let mut ba: Vec<serde_json::Value> = Vec::new();
                    let mut bk: Vec<serde_json::Value> = Vec::new();
                    amount += trn._get_f64("debit").unwrap();
                    if [
                        "TRADE_PAYABLE",
                        "TRADE_RECEIVABLE",
                        "ACCOUNT_PAYABLE",
                        "ACCOUNT_RECEIVABLE",
                    ]
                    .contains(&trn.get_str("accountType").unwrap())
                    {
                        ba.push(serde_json::json!( {
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
                        bk.push(serde_json::json!({
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
                    let ac_trn = serde_json::json!({
                        "id": Uuid::new_v4(),
                        "account": account,
                        "debit": trn._get_f64("debit").unwrap(),
                        "credit": trn._get_f64("credit").unwrap(),
                        "is_default": trn.get_bool("isDefault").ok(),
                        // "gst_tax": trn.get_string("tax"),
                        "taxable_amount": trn._get_f64("taxableAmount"),
                        "cgst_amount": trn._get_f64("cgstAmount"),
                        "igst_amount": trn._get_f64("igstAmount"),
                        "sgst_amount": trn._get_f64("sgstAmount"),
                        "cess_amount": trn._get_f64("cessAmount"),
                        "qty": trn._get_f64("qty"),
                        "bill_allocations": (!ba.is_empty()).then_some(ba),
                        "bank_allocations": (!bk.is_empty()).then_some(bk),
                    });
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
                        party_gst
                    ) VALUES 
                    ($1::TEXT::DATE, $2::TEXT::DATE, $3, $4, $5::TEXT::typ_voucher_mode, $6, $7, 
                        $8::JSONB, $9, $10, $11, $12, $13::TEXT::typ_base_voucher_type,$14,$15,$16,$17,
                        $18::JSON, $19::JSON)",
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
                        &party_gst
                    ],
                )
                .await
                .unwrap();
            }
            println!("end {}...", collection);
        }
    }
}
