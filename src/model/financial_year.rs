use super::*;

pub struct FinancialYear;

impl FinancialYear {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("financial_years")
            .find(doc! {}, None)
            .await
            .unwrap();
        let mut updates = Vec::new();
        postgres
            .execute("DELETE FROM financial_year", &[])
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            let id : i32 = postgres
                .query_one(
                    "INSERT INTO financial_year (fy_start,fy_end) VALUES ($1::text::date, $2::text::date) returning id",
                    &[
                        &d.get_str("fStart").unwrap(),
                        &d.get_str("fEnd").unwrap(),
                    ],
                )
                .await
                .unwrap().get(0);
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
