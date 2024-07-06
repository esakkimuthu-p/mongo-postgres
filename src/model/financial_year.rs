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
        postgres
            .execute("DELETE FROM financial_year", &[])
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            id += 1;
            postgres
                .execute(
                    "INSERT INTO financial_year (id,fy_start,fy_end) OVERRIDING SYSTEM VALUE VALUES ($1, $2::TEXT::DATE, $3::TEXT::DATE)",
                    &[
                        &id,
                        &d.get_str("fStart").unwrap(),
                        &d.get_str("fEnd").unwrap(),
                    ],
                )
                .await
                .unwrap();
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "financial_years",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
