use super::*;

pub struct Salt;

impl Salt {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("salts")
            .find(doc! {}, None)
            .await
            .unwrap();
        let mut updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            let drug = d
                .get_string("h")
                .or(d.get_string("h1"))
                .or(d.get_string("nc"));
            let id: i32 = postgres
                .query_one(
                    "INSERT INTO pharma_salt (name, drug_category) VALUES ($1,$2) returning id",
                    &[&d.get_str("name").unwrap(), &drug],
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
