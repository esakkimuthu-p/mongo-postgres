use super::*;

pub struct Salt;

impl Salt {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("pharma_salts")
            .find(doc! {}, None)
            .await
            .unwrap();
        let mut updates = Vec::new();
        postgres
            .execute(
                "insert into tag (name) values ('Schedule H'), ('Schedule H1'), ('Narcotics');",
                &[],
            )
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            let id: i32 = postgres
                .query_one(
                    "INSERT INTO tag (name) VALUES ($1) returning id",
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
