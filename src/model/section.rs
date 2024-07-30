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
        let mut updates = Vec::new();
        postgres
            .execute(
                "update category set category = 'Sections' where id = 'INV_CAT1'",
                &[],
            )
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            let id: i32 = postgres
                .query_one(
                    "INSERT INTO category_option (name, category_id, active) VALUES ($1, 'INV_CAT1', true) returning id",
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
                "update": "sections",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
