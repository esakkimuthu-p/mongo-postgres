use super::*;

pub struct Member;

impl Member {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("members")
            .find(
                doc! {},
                find_opts(
                    doc! {"createdBy": 0, "createdAt": 0, "updatedAt": 0, "updatedBy": 0},
                    doc! {"isRoot": -1, "_id": 1},
                ),
            )
            .await
            .unwrap();
        let mut id: i32 = 0;
        let mut updates = Vec::new();
        let mut ref_branch_updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            id += 1;
            let perms = d
                .get_array("permissions")
                .unwrap_or(&vec![])
                .iter()
                .map(|x| x.as_str().unwrap().to_lowercase())
                .collect::<Vec<String>>();
            postgres
                .execute(
                    "INSERT INTO members 
                        (id,name,user_id, pass,nick_name,remote_access, is_root, perms)
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
                        &(!perms.is_empty()).then_some(perms),
                    ],
                )
                .await
                .unwrap();
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
            ref_branch_updates.push(doc! {
                "q": { "members": object_id },
                "u": { "$addToSet": { "postgresMembers": id} },
                "multi": true
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "members",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
            let command = doc! {
                "update": "branches",
                "updates": &ref_branch_updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
