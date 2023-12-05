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
        let mut id: i32 = 0;
        let mut updates = Vec::new();
        let mut inv_updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            id += 1;
            postgres
                .execute(
                    "INSERT INTO manufacturers (id,name,display_name, val_name, mobile, email) OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3, $4, $5, $6)",
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &d.get_str("displayName").unwrap(),
                        &val_name(d.get_str("name").unwrap()),
                        &d.get_str("mobile").ok(),
                        &d.get_str("email").ok(),
                    ],
                )
                .await
                .unwrap();
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
            inv_updates.push(doc! {
                "q": { "manufacturerId": object_id },
                "u": { "$set": { "postgresManuf": id} },
                "multi": true,
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "manufacturers",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
        if !inv_updates.is_empty() {
            let command = doc! {
                "update": "inventories",
                "updates": &inv_updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
