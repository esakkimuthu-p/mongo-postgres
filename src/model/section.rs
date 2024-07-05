use super::*;

pub struct Section;

impl Section {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("sections")
            .find(
                doc! {},
                find_opts(
                    doc! {"createdBy": 0, "createdAt": 0, "updatedAt": 0, "updatedBy": 0},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap();
        let mut id: i64 = 0;
        let mut updates = Vec::new();
        let mut parent_ref_updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            id += 1;
            postgres
                .execute(
                    "INSERT INTO sections (id,name) OVERRIDING SYSTEM VALUE VALUES ($1, $2)",
                    &[&id, &d.get_str("name").unwrap()],
                )
                .await
                .unwrap();
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
            parent_ref_updates.push(doc! {
                "q": { "parentSection": object_id },
                "u": { "$set": { "postgresParent": id} },
                "multi": true
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "sections",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();

            let command = doc! {
                "update": "sections",
                "updates": &parent_ref_updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
        let mut cur = mongodb
            .collection::<Document>("sections")
            .find(
                doc! {"postgresParent":{"$exists":true}},
                find_opts(
                    doc! {"_id": 0, "postgres": 1, "postgresParent": 1},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            postgres
                .execute(
                    "UPDATE sections SET parent = $2 WHERE id = $1",
                    &[
                        &d._get_i32("postgres").unwrap(),
                        &d._get_i32("postgresParent").unwrap(),
                    ],
                )
                .await
                .unwrap();
        }
    }
}
