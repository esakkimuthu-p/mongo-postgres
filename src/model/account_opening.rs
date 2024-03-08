use mongodb::bson::Uuid;

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
                            x.get_i32("postgres").unwrap(),
                            x.get_str("postgresAccountType").unwrap(),
                        ))
                })
                .unwrap();
            let branch = branches
                .iter()
                .find_map(|x| {
                    (x.get_object_id("_id").unwrap() == d.get_object_id("branch").unwrap())
                        .then_some(x.get_i32("postgres").unwrap())
                })
                .unwrap();
            let mut ba: Vec<serde_json::Value> = Vec::new();
            if ["SUNDRY_CREDITOR", "SUNDRY_DEBTOR"].contains(&account_type) {
                ba.push(serde_json::json!({
                    "id": Uuid::new().to_string(),
                    "amount": d._get_f64("debit").unwrap() - d._get_f64("credit").unwrap(),
                    "ref_type": "ON_ACC",
                    "ref_no": "OPENING",
                }));
            }
            postgres
                .execute(
                    "INSERT INTO account_openings (account,branch, credit, debit, bill_allocations, id) 
                    VALUES ($1, $2, $3, $4, $5::JSONB, gen_random_uuid())",
                    &[
                        &account,
                        &branch,
                        &d._get_f64("credit").unwrap(),
                        &d._get_f64("debit").unwrap(),
                        &(!ba.is_empty()).then_some(serde_json::to_value(ba).unwrap())
                    ],
                )
                .await
                .unwrap();
        }
    }
}
