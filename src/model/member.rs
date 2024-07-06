use super::*;

pub struct Member;

impl Member {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("members")
            .find(
                doc! {"isRoot": false},
                find_opts(
                    doc! {"createdBy": 0, "createdAt": 0, "updatedAt": 0, "updatedBy": 0},
                    doc! {"isRoot": -1, "_id": 1},
                ),
            )
            .await
            .unwrap();
        let mut id: i32 = 1;
        let mut updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            id += 1;
            postgres
                .execute(
                    "INSERT INTO member
                        (id,name,user_id, pass,nick_name,remote_access, is_root)
                    OVERRIDING SYSTEM VALUE
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                    &[
                        &id,
                        &d.get_str("username").unwrap(),
                        &d.get_object_id("user").ok().map(|x| x.to_hex()),
                        &d.get_str("username").unwrap(),
                        &d.get_str("nickName").ok(),
                        &d.get_bool("remoteAccess").unwrap_or_default(),
                        &d.get_bool("isRoot").unwrap_or_default(),
                    ],
                )
                .await
                .unwrap();
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
        }
        updates.push(doc! {
            "q": { "isRoot": true },
            "u": { "$set": { "postgres": 1} },
        });
        let command = doc! {
            "update": "members",
            "updates": &updates
        };
        mongodb.run_command(command, None).await.unwrap();
    }
}
