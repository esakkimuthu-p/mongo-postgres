use super::*;

pub struct GstRegistration;

impl GstRegistration {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("gst_registrations")
            .find(
                doc! {},
                find_opts(
                    doc! {"createdBy": 0, "createdAt": 0, "updatedAt": 0, "updatedBy": 0},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap();
        let mut updates = Vec::new();
        let mut ref_updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            let gst_no = d.get_str("gstNo").unwrap_or_default();
            let id: i32 = postgres
                .query_one(
                    "INSERT INTO gst_registration 
                    (gst_no, state_id, username,email,e_invoice_username, e_password) 
                    VALUES 
                    ($1, $2, $3, $4, $5, $6) returning id",
                    &[
                        &d.get_str("gstNo").unwrap_or_default(),
                        &"33",
                        &d.get_str("username").ok(),
                        &d.get_str("email").ok(),
                        &d.get_str("eInvoiceUsername").ok(),
                        &d.get_str("ePassword").ok(),
                    ],
                )
                .await
                .unwrap()
                .get(0);
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
            ref_updates.push(doc! {
                "q": { "gstInfo.gstNo": &gst_no },
                "u": { "$set": { "postgresGst": id} },
                "multi": true,
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "gst_registrations",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
            let command = doc! {
                "update": "branches",
                "updates": &ref_updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
