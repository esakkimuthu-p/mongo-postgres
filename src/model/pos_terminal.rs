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
        let mut id: i64 = 0;
        while let Some(Ok(d)) = cur.next().await {
            id += 1;
            let branch = branches
                .iter()
                .find_map(|x| {
                    (x.get_object_id("_id").unwrap() == d.get_object_id("branch").unwrap())
                        .then_some(x._get_i32("postgres").unwrap())
                })
                .unwrap();
            postgres
                .execute(
                    "INSERT INTO pos_server (id,name,branch_id,mode,is_active)
                     OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3, $4, $5)",
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &branch,
                        &d.get_str("mode").unwrap(),
                        &d.get_bool("isActive").unwrap_or(true),
                    ],
                )
                .await
                .unwrap();
        }
    }
}
