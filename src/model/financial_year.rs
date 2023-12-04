use super::*;

pub struct FinancialYear;

impl FinancialYear {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("financial_years")
            .find(doc! {}, None)
            .await
            .unwrap();
        let mut id: i32 = 0;
        let mut updates = Vec::new();
        let mut ref_updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            id += 1;
            postgres
                .execute(
                    "INSERT INTO financial_years (id,fy_start,fy_end) VALUES ($1, $2, $3)",
                    &[
                        &id,
                        &NaiveDate::from_str(d.get_str("fStart").unwrap()).unwrap(),
                        &NaiveDate::from_str(d.get_str("fEnd").unwrap()).unwrap(),
                    ],
                )
                .await
                .unwrap();
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
            ref_updates.push(doc! {
                "q": { "fYear":  object_id  },
                "u": { "$set": { "fyPostgres": id} },
                "multi": true,
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "financial_years",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
        if !ref_updates.is_empty() {
            let command = doc! {
                "update": "voucher_numberings",
                "updates": &ref_updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
