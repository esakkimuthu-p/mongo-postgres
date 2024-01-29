use super::*;

pub struct Voucher;

impl Voucher {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("payments")
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
        let mut inv_updates = Vec::new();
        // Object {"ac_trns": Array [Object {"account": Number(1), "bill_allocations": Array [Object {"amount": Number(10.0), "eff_date": String("2025-01-01"), "id": String("489fb37c-036b-44df-9dbc-2e55c1b77121"), "pending": Null, "pending_id": Null, "ref_no": String("123"), "ref_type": String("NEW")}], "credit": Number(10.0), "debit": Number(0.0), "id": String("b4fb01e4-9d5f-4fd5-9cc5-d0c363e163cb")}], "amount": Null, "branch": Number(1), "branch_gst": Null, "change_voucher_no": Null, "date": String("2024-01-01"), "description": Null, "eff_date": String("2024-01-01"), "lut": Bool(true), "mode": String("Account"), "particulars": String("particular"), "party": Null, "party_gst": Null, "rcm": Null, "ref_no": String("123"), "tds_detail": Null, "unique_session": Null, "voucher_type": Number(1)}
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            id += 1;
            postgres
                .execute(
                    "INSERT INTO manufacturers (id,name, mobile, email, telephone) OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3, $4, $5)",
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &d.get_str("mobile").ok(),
                        &d.get_str("email").ok(),
                        &d.get_str("telephone").ok(),
                    ],
                )
                .await
                .unwrap();
            updates.push(doc! {
                "q": { "_id": object_id },
                "u": { "$set": { "postgres": id} },
            });
            inv_updates.push(doc! {
                "q": { "manufacturerId": object_id },
                "u": { "$set": { "postgresMan": id} },
                "multi": true,
            });
        }
        if !updates.is_empty() {
            let command = doc! {
                "update": "manufacturers",
                "updates": &updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
        if !inv_updates.is_empty() {
            let command = doc! {
                "update": "inventories",
                "updates": &inv_updates
            };
            mongodb.run_command(command, None).await.unwrap();
        }
    }
}
