use serde_json::json;

use super::*;

pub struct Member;

impl Member {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient, jwt: &String) {
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
        let mut updates = Vec::new();
        let js = serde_json::json!({"jwt_private_key": jwt});
        let perms = vec!["member_role__select", "bank_beneficiary__select"];
        let ui_perms = json!([
            "adm.role.vw",
            "adm.role.cr",
            "adm.role.up",
            "inv.inv.vw",
            "inv.inv.cr",
            "inv.inv.up",
            "rpt.sltg",
            "inv.sb.vw",
            "inv.sb.cr",
            "rpt.sls.slreg",
            "inv.sb.up"
        ]);
        postgres
            .execute(
                "select set_config('app.env',($1)::json::text,false)",
                &[&js],
            )
            .await
            .unwrap();
        postgres
            .execute(
                "insert into member_role(name, perms, ui_perms)
                values ('custom', $1::text[], $2::json)",
                &[&perms, &ui_perms],
            )
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            let id: i32 = postgres
                .query_one(
                    "INSERT INTO member
                        (name,user_id, pass,nick_name,remote_access, is_root, role_id)
                    VALUES ($1, $2, $3, $4, $5, $6, 'custom') returning id",
                    &[
                        &d.get_str("username").unwrap(),
                        &d.get_object_id("user").ok().map(|x| x.to_hex()),
                        &d.get_str("username").unwrap(),
                        &d.get_str("nickName").ok(),
                        &d.get_bool("remoteAccess").unwrap_or_default(),
                        &d.get_bool("isRoot").unwrap_or_default(),
                    ],
                )
                .await
                .unwrap()
                .get(0);
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
