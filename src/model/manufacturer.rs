use super::*;

pub struct Manufacturer;

impl Manufacturer {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("manufacturers")
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
            let id : i32 =postgres
                .query_one(
                    "INSERT INTO manufacturer (name, mobile, email, telephone) VALUES ($1, $2, $3, $4) returning id",
                    &[
                        &d.get_str("name").unwrap(),
                        &d.get_str("mobile").ok(),
                        &d.get_str("email").ok(),
                        &d.get_str("telephone").ok(),
                    ],
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
                "update": "manufacturers",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
