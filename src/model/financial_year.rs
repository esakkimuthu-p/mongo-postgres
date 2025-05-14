use super::*;

pub struct FinancialYear;

impl FinancialYear {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let cur = mongodb
            .collection::<Document>("financial_years")
            .find_one(doc! {"fStart": "2025-04-01"}, None)
            .await
            .unwrap();
        let mut updates = Vec::new();
        if let Some(d) = &cur {
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
