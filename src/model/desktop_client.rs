use super::*;

pub struct DesktopClient;

impl DesktopClient {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("desktop_clients")
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
            let branches = d
                .get_array("postgresBranches")
                .unwrap_or(&vec![])
                .iter()
                .map(|x| x.as_i32().unwrap())
                .collect::<Vec<i32>>();
            postgres
                .execute(
                    "INSERT INTO desktop_clients (id,name,display_name,val_name,branches,access) OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3, $4, $5, $6`)",
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &d.get_str("display_name").unwrap(),
                        &val_name(d.get_str("name").unwrap()),
                        &branches,
                        &d.get_bool("access").ok(),
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
                "update": "desktop_clients",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
