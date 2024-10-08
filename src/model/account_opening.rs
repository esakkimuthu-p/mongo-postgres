use super::*;

pub struct AccountOpening;

impl AccountOpening {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
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
