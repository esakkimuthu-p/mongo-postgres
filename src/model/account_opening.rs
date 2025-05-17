use super::*;

pub struct AccountOpening;

impl AccountOpening {
    pub async fn new_create(mongodb: &Database, postgres: &PostgresClient) {
        let accounts = mongodb
            .collection::<Document>("accounts")
            .find(
                doc! {},
                find_opts(
                    doc! {"_id": 1, "postgres": 1, "postgresAccountType": 1},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let branches = mongodb
            .collection::<Document>("branches")
            .find(
                doc! {},
                find_opts(doc! {"_id": 1, "postgres": 1}, doc! {"_id": 1}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();

        mongodb
            .collection::<Document>("bill_allocations")
            .aggregate(
                vec![
                    doc! {
                        "$match": {
                            "date": { "$lt": "2025-01-01" }
                        }
                    },
                    doc! {
                        "$group": {
                            "_id": "$pending",
                            "closing": { "$sum": "$amount" },
                            "new_refs": {
                                "$push": {
                                    "$cond": [
                                        { "$eq": ["$refType", "NEW"] },
                                        {"ref_no": "$refNo", "voucher_no": "$voucherNo","voucher_type": "$voucherType", "voucher_date": "$date", "eff_date": "$effDate", "branch_id": "$branch", "account_id": "$account" },
                                        "$$REMOVE"
                                    ]
                                }
                            }
                        }
                    },
                    doc! {"$unwind": "$new_refs"},
                    doc! { "$addFields": {
                            "closing": { "$round": ["$closing", 2] },
                            "voucher_no": "$new_refs.voucher_no",
                            "voucher_date": "$new_refs.voucher_date",
                            "eff_date": "$new_refs.eff_date",
                            "branch_id": "$new_refs.branch_id",
                            "account_id": "$new_refs.account_id",
                            "ref_no": "$new_refs.ref_no",
                            "voucher_type": "$new_refs.voucher_type",
                    }},
                    doc! {"$match": { "closing": { "$ne": 0 } }},
                    doc! {
                        "$project": {
                            "_id": 1,
                            "branch_id": 1,
                            "account_id": 1,
                            "closing": 1,
                            "voucher_date": 1,
                            "voucher_no": 1,
                            "eff_date": 1,
                            "ref_no": 1,
                            "voucher_type": 1,
                        }
                    },
                    doc! {
                        "$out": "script_bill_opening"
                    },
                ],
                None,
            )
            .await
            .unwrap();
        let mut cur = mongodb
            .collection::<Document>("account_transactions")
            .aggregate(
                vec![
                    doc! {
                        "$match": {
                            "date": { "$lt": "2025-01-01" },
                            "accountType": {"$ne": "STOCK"}
                        }
                    },
                    doc! {
                        "$group": {
                            "_id": { "branch_id": "$branch", "account_id": "$account" },
                            "closing": { "$sum": { "$subtract": ["$debit", "$credit"] } }
                        }
                    },
                    doc! {
                        "$addFields": {
                            "closing": { "$round": ["$closing", 2] },
                            "branch_id": "$_id.branch_id",
                            "account_id": "$_id.account_id"
                        }
                    },
                    doc! {
                        "$match": { "closing": { "$ne": 0 } }
                    },
                    doc! {
                        "$project": {
                            "_id": 0,
                            "branch_id": 1,
                            "account_id": 1,
                            "closing": 1
                        }
                    },
                ],
                None,
            )
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            let (account, account_type) = accounts
                .iter()
                .find_map(|x| {
                    (x.get_object_id("_id").unwrap() == d.get_object_id("account_id").unwrap())
                        .then_some((
                            x._get_i32("postgres").unwrap(),
                            x._get_i32("postgresAccountType").unwrap(),
                        ))
                })
                .unwrap();
            let branch = branches
                .iter()
                .find_map(|x| {
                    (x.get_object_id("_id").unwrap() == d.get_object_id("branch_id").unwrap())
                        .then_some(x._get_i32("postgres").unwrap())
                })
                .unwrap();
            let mut ba: Vec<serde_json::Value> = Vec::new();
            let closing = d._get_f64("closing").unwrap();
            let (dr, cr) = if closing > 0.0 {
                (closing, 0.0)
            } else {
                (0.0, closing.abs())
            };
            if [16, 19].contains(&account_type) {
                let mut on_acc_val = closing;
                let allocs = mongodb
                                .collection::<Document>("script_bill_opening")
                                .find(
                                    doc! {"account_id": d.get_object_id("account_id").unwrap(), "branch_id": d.get_object_id("branch_id").unwrap()},
                                    None,
                                )
                                .await
                                .unwrap()
                                .try_collect::<Vec<Document>>()
                                .await
                                .unwrap();
                for alloc in allocs {
                    let oid = alloc.get_object_id("_id").unwrap().to_hex();
                    let pending = format!(
                        "{}-{}-4{}-{}-{}4444444",
                        oid[0..8].to_owned(),
                        oid[8..12].to_owned(),
                        oid[12..15].to_owned(),
                        oid[15..19].to_owned(),
                        oid[19..24].to_owned(),
                    );
                    let amount = alloc._get_f64("closing").unwrap();
                    on_acc_val -= amount;
                    ba.push(serde_json::json!({
                        "pending": pending,
                        "amount": amount,
                        "ref_type": "NEW",
                        "ref_no": format!("vNo: {}, vTy: {}", alloc.get_string("voucher_no").or(alloc.get_string("ref_no")).unwrap_or("OPENING".to_string()), alloc.get_string("voucher_type").unwrap_or_default()),
                    }));
                }
                if round64(on_acc_val, 2) != 0.0 {
                    ba.push(serde_json::json!({
                        "amount": on_acc_val,
                        "ref_type": "ON_ACC",
                        "ref_no": "On acc value"
                    }));
                }
            }
            let data = serde_json::json!(
                {
                    "account_id": account, "branch_id": branch, "credit": cr, "debit": dr,
                    "bill_allocations": (!ba.is_empty()).then_some(serde_json::to_value(ba).unwrap())
                }
            );
            postgres
                .execute("select * from set_account_opening($1::json)", &[&data])
                .await
                .unwrap();
        }
    }
    pub async fn _create(mongodb: &Database, postgres: &PostgresClient) {
        let accounts = mongodb
            .collection::<Document>("accounts")
            .find(
                doc! {},
                find_opts(
                    doc! {"_id": 1, "postgres": 1, "postgresAccountType": 1},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let branches = mongodb
            .collection::<Document>("branches")
            .find(
                doc! {},
                find_opts(doc! {"_id": 1, "postgres": 1}, doc! {"_id": 1}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        let mut cur = mongodb
            .collection::<Document>("account_openings")
            .find(
                doc! {},
                find_opts(
                    doc! {"createdBy": 0, "createdAt": 0, "updatedAt": 0, "updatedBy": 0},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            let (account, account_type) = accounts
                .iter()
                .find_map(|x| {
                    (x.get_object_id("_id").unwrap() == d.get_object_id("account").unwrap())
                        .then_some((
                            x._get_i32("postgres").unwrap(),
                            x._get_i32("postgresAccountType").unwrap(),
                        ))
                })
                .unwrap();
            let branch = branches
                .iter()
                .find_map(|x| {
                    (x.get_object_id("_id").unwrap() == d.get_object_id("branch").unwrap())
                        .then_some(x._get_i32("postgres").unwrap())
                })
                .unwrap();
            let mut ba: Vec<serde_json::Value> = Vec::new();
            if [16, 19].contains(&account_type) {
                let mut on_acc_val = d._get_f64("debit").unwrap() - d._get_f64("credit").unwrap();
                let allocs = mongodb
                                .collection::<Document>("bill_allocations")
                                .find(
                                    doc! {"txnId": d.get_object_id("_id").unwrap()},
                                    find_opts(
                                        doc! {"txnId": 1, "amount": 1, "refNo": 1, "pending": 1, "refType": 1, "_id": 0},
                                        doc! {},
                                    ),
                                )
                                .await
                                .unwrap()
                                .try_collect::<Vec<Document>>()
                                .await
                                .unwrap();
                for alloc in allocs {
                    let oid = alloc.get_object_id("pending").unwrap().to_hex();
                    let pending = format!(
                        "{}-{}-4{}-{}-{}4444444",
                        oid[0..8].to_owned(),
                        oid[8..12].to_owned(),
                        oid[12..15].to_owned(),
                        oid[15..19].to_owned(),
                        oid[19..24].to_owned(),
                    );
                    let amount = alloc._get_f64("amount").unwrap();
                    on_acc_val -= amount;
                    ba.push(serde_json::json!({
                        "pending": pending,
                        "amount": amount,
                        "ref_type": alloc.get_str("refType").unwrap(),
                        "ref_no": alloc.get_string("refNo").or(d.get_string("refNo")),
                    }));
                }
                if round64(on_acc_val, 2) != 0.0 {
                    ba.push(serde_json::json!({
                        "amount": on_acc_val,
                        "ref_type": "ON_ACC",
                        "ref_no": "On acc value"
                    }));
                }
            }
            let data = serde_json::json!(
                {
                    "account_id": account, "branch_id": branch, "credit": d._get_f64("credit").unwrap(), "debit": d._get_f64("debit").unwrap(),
                    "bill_allocations": (!ba.is_empty()).then_some(serde_json::to_value(ba).unwrap())
                }
            );
            postgres
                .execute("select * from set_account_opening($1::json)", &[&data])
                .await
                .unwrap();
        }
    }
}
