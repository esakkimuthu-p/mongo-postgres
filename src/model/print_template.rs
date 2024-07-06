use super::*;

pub struct PrintTemplate;

impl PrintTemplate {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("print_templates")
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
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            id += 1;
            postgres
                .execute(
                    "INSERT INTO print_template (id,name,template,layout,voucher_mode) 
                    OVERRIDING SYSTEM VALUE VALUES 
                    ($1, $2, $3, $4, $5)",
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &d.get_str("template").unwrap(),
                        &d.get_str("layout").unwrap(),
                        &d.get_str("voucherMode").ok().map(|x| &x[0..2]),
                    ],
                )
                .await
                .unwrap();
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "print_templates",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
