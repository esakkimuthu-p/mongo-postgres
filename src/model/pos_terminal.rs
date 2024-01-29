use super::*;

pub struct PosTerminal;

impl PosTerminal {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("pos_terminals")
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
            let members = d
                .get_array("postgresmembers")
                .map(|x| x.iter().map(|x| x.as_i32().unwrap()).collect::<Vec<i32>>())
                .ok();
            postgres
                .execute(
                    "INSERT INTO pos_terminals (id,name,pass,branch,members,mode,configuration)
                     OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3, $4, $5, $6::TEXT::typ_pos_mode, $7)",
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &d.get_str("password").unwrap(),
                        &d.get_str("postgresBranch").unwrap(),
                        &members,
                        &d.get_str("mode").unwrap(),
                        &"{}"
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
                "update": "pos_terminals",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
