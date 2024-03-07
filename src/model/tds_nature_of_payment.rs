use super::*;

pub struct TdsNatureOfPayment;

impl TdsNatureOfPayment {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("tds_nature_of_payments")
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
                    "INSERT INTO tds_nature_of_payments 
                    (id,name,section,ind_huf_rate,ind_huf_rate_wo_pan,other_deductee_rate,other_deductee_rate_wo_pan,threshold) 
                    OVERRIDING SYSTEM VALUE
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &d.get_str("section").ok(),
                        &d._get_f64("indHufRate").unwrap_or_default(),
                        &d._get_f64("indHufRateWoPan").unwrap_or_default(),
                        &d._get_f64("otherDeducteeRate").unwrap_or_default(),
                        &d._get_f64("otherDeducteeRateWoPan").unwrap_or_default(),
                        &d._get_f64("threshold").unwrap_or_default(),
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
                "update": "tds_nature_of_payments",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
