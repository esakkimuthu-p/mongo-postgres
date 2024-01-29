use super::*;

pub struct Doctor;

impl Doctor {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("doctors")
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
        let mut sale_updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            id += 1;
            postgres
                .execute(
                    "INSERT INTO doctors (id,name,license_no) OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3)",
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &d.get_str("licenseNo").ok(),
                    ],
                )
                .await
                .unwrap();
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
            sale_updates.push(doc! {
                "q": { "doctor": object_id },
                "u": { "$set": { "postgresDoctor": id} },
                "multi": true,
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "doctors",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
        if !sale_updates.is_empty() {
            let command = doc! {
                "update": "sales",
                "updates": &sale_updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
