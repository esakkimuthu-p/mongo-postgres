use super::*;

pub struct Salt;

impl Salt {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("pharma_salts")
            .find(
                doc! {},
                find_opts(
                    doc! {"createdBy": 0, "createdAt": 0, "updatedAt": 0, "updatedBy": 0},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap();
        let mut updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            let id: i32 = postgres
                .query_one(
                    "INSERT INTO pharma_salt (name) VALUES ($1) returning id",
                    &[&d.get_str("name").unwrap()],
                )
                .await
                .unwrap()
                .get(0);
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "pharma_salts",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
