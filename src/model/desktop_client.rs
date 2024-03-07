use mongodb::bson::oid::ObjectId;

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
        let branches = mongodb
            .collection::<Document>("branches")
            .find(
                doc! {},
                find_opts(doc! {"_id": 1, "postgres": 1}, doc! {"_id": 1}),
            )
            .await
            .unwrap()
            .try_collect::<Vec<Document>>()
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            let mut br_ids = Vec::new();
            let dt_branches = d
                .get_array("branches")
                .unwrap_or(&vec![])
                .iter()
                .map(|x| x.as_object_id().unwrap())
                .collect::<Vec<ObjectId>>();
            for x in dt_branches {
                let br = branches
                    .iter()
                    .find_map(|y| {
                        (y.get_object_id("_id").unwrap() == x)
                            .then_some(y.get_i32("postgres").unwrap())
                    })
                    .unwrap();
                br_ids.push(br);
            }
            postgres
                .execute(
                    "INSERT INTO desktop_clients (name,branches,access) 
                    OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3)",
                    &[
                        &d.get_str("name").unwrap(),
                        &br_ids,
                        &d.get_bool("access").ok(),
                    ],
                )
                .await
                .unwrap();
        }
    }
}
