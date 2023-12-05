use super::*;

pub struct PharmaSalt;

impl PharmaSalt {
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
        let mut id: i32 = 0;
        let mut updates = Vec::new();
        let mut ref_inv_updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            id += 1;
            postgres
                .execute(
                    "INSERT INTO pharma_salts (id,name,display_name, val_name) OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3, $4)",
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &d.get_str("displayName").unwrap(),
                        &val_name(d.get_str("name").unwrap()),
                    ],
                )
                .await
                .unwrap();
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
            ref_inv_updates.push(doc! {
                "q": { "salts": object_id },
                "u": { "$addToSet": { "postgresSalts": id} },
                "multi": true
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "pharma_salts",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
            let command = doc! {
                "update": "inventories",
                "updates": &ref_inv_updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
