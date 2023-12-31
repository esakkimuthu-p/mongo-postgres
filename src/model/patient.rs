use super::*;

pub struct Patient;

impl Patient {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("patients")
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
                    "INSERT INTO patients (id,name,display_name, val_name, customer) OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3, $4, $5)",
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &d.get_str("displayName").unwrap(),
                        &val_name(d.get_str("name").unwrap()),
                        &d.get_i32("postgresContact").unwrap(),
                    ],
                )
                .await
                .unwrap();
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
            sale_updates.push(doc! {
                "q": { "patient": object_id },
                "u": { "$set": { "postgresPatient": id} },
                "multi": true,
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "patients",
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
