use super::*;

pub struct Unit;

impl Unit {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("units")
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
        let mut inv_unit_updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            id += 1;
            postgres
                .execute(
                    "INSERT INTO units (id,name,uqc,symbol,precision) OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3, $4, $5::INT::SMALLINT)",
                    &[
                        &id, 
                        &d.get_str("name").unwrap(), 
                        &d.get_str("uqc").unwrap(), 
                        &d.get_str("symbol").unwrap(), 
                        &0
                    ],
                )
                .await
                .unwrap();
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
            inv_unit_updates.push(doc! {
                "q": { "units": {"$elemMatch": {"unitId": object_id }} },
                "u": { "$set": { "units.$[elm].postgresUnit": id} },
                "multi": true,
                "arrayFilters": [ { "elm.unitId": {"$eq":object_id} } ]
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "units",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
            let command = doc! {
                "update": "inventories",
                "updates": &inv_unit_updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
