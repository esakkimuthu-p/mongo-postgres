use super::*;

pub struct VoucherType;

impl VoucherType {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let base_types: [&str; 8] = [
            "PAYMENT",
            "RECEIPT",
            "CONTRA",
            "JOURNAL",
            "SALE",
            "PURCHASE",
            "CREDIT_NOTE",
            "DEBIT_NOTE",
        ];
        let mut cur = mongodb
            .collection::<Document>("voucher_types")
            .find(
                doc! {"voucherType": {"$in": base_types.to_vec() }},
                find_opts(
                    doc! {"createdBy": 0, "createdAt": 0, "updatedAt": 0, "updatedBy": 0},
                    doc! {"_id": 1},
                ),
            )
            .await
            .unwrap();
        let mut id: i32 = 20;
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();

            let members = d
                .get_array("postgresMembers")
                .map(|x| x.iter().map(|x| x.as_i32().unwrap()).collect::<Vec<i32>>())
                .ok();
            let config = match d.get_str("voucherType").unwrap() {
                "PAYMENT" => {
                    let mut type_id = 1;
                    if !d.get_bool("default").unwrap_or_default() {
                        id += 1;
                        type_id = id;
                    }

                    mongodb
                        .collection::<Document>("payments")
                        .update_many(
                            doc! {"voucherTypeId": object_id},
                            doc! {"$set": {"postgresVoucherType": type_id}},
                            None,
                        )
                        .await
                        .unwrap();
                    serde_json::json!({ "payments": {"printAfterSave": false }})
                }
                "RECEIPT" => {
                    let mut type_id = 2;
                    if !d.get_bool("default").unwrap_or_default() {
                        id += 1;
                        type_id = id;
                    }
                    mongodb
                        .collection::<Document>("receipts")
                        .update_many(
                            doc! {"voucherTypeId": object_id},
                            doc! {"$set": {"postgresVoucherType": type_id}},
                            None,
                        )
                        .await
                        .unwrap();
                    serde_json::json!({ "receipt": {"printAfterSave": false }})
                }
                "CONTRA" => {
                    let mut type_id = 3;
                    if !d.get_bool("default").unwrap_or_default() {
                        id += 1;
                        type_id = id;
                    }
                    mongodb
                        .collection::<Document>("contras")
                        .update_many(
                            doc! {"voucherTypeId": object_id},
                            doc! {"$set": {"postgresVoucherType": type_id}},
                            None,
                        )
                        .await
                        .unwrap();
                    serde_json::json!({ "contra": {"printAfterSave": false }})
                }
                "JOURNAL" => {
                    let mut type_id = 4;
                    if !d.get_bool("default").unwrap_or_default() {
                        id += 1;
                        type_id = id;
                    }
                    mongodb
                        .collection::<Document>("journals")
                        .update_many(
                            doc! {"voucherTypeId": object_id},
                            doc! {"$set": {"postgresVoucherType": type_id}},
                            None,
                        )
                        .await
                        .unwrap();
                    serde_json::json!({ "journal": {"printAfterSave": false }})
                }
                "SALE" => {
                    let mut type_id = 5;
                    if !d.get_bool("default").unwrap_or_default() {
                        id += 1;
                        type_id = id;
                    }
                    mongodb
                        .collection::<Document>("sales")
                        .update_many(
                            doc! {"voucherTypeId": object_id},
                            doc! {"$set": {"postgresVoucherType": type_id}},
                            None,
                        )
                        .await
                        .unwrap();
                    serde_json::json!({ "sale": {"account": {"printAfterSave": false }}})
                }
                "CREDIT_NOTE" => {
                    let mut type_id = 6;
                    if !d.get_bool("default").unwrap_or_default() {
                        id += 1;
                        type_id = id;
                    }
                    mongodb
                        .collection::<Document>("credit_notes")
                        .update_many(
                            doc! {"voucherTypeId": object_id},
                            doc! {"$set": {"postgresVoucherType": type_id}},
                            None,
                        )
                        .await
                        .unwrap();
                    serde_json::json!({ "credit_note": {"account": {"printAfterSave": false }}})
                }
                "PURCHASE" => {
                    let mut type_id = 7;
                    if !d.get_bool("default").unwrap_or_default() {
                        id += 1;
                        type_id = id;
                    }
                    mongodb
                        .collection::<Document>("purchases")
                        .update_many(
                            doc! {"voucherTypeId": object_id},
                            doc! {"$set": {"postgresVoucherType": type_id}},
                            None,
                        )
                        .await
                        .unwrap();
                    serde_json::json!({ "purchase": {"account": {"printAfterSave": false }}})
                }

                "DEBIT_NOTE" => {
                    let mut type_id = 8;
                    if !d.get_bool("default").unwrap_or_default() {
                        id += 1;
                        type_id = id;
                    }
                    mongodb
                        .collection::<Document>("debit_notes")
                        .update_many(
                            doc! {"voucherTypeId": object_id},
                            doc! {"$set": {"postgresVoucherType": type_id}},
                            None,
                        )
                        .await
                        .unwrap();
                    serde_json::json!({ "debit_note": {"account": {"printAfterSave": false }}})
                }
                _ => panic!("Invalid voucherTypes"),
            };
            if !d.get_bool("default").unwrap_or_default() {
                postgres
                .execute(
                    "INSERT INTO voucher_types (id, name, base_type, config, members, prefix)
                     OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3::TEXT::typ_base_voucher_type, $4, $5, $6)",
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &d.get_str("voucherType").unwrap(),
                        &config,
                        &members,
                        &d.get_str("prefix").ok(),
                    ],
                )
                .await
                .unwrap();
            }
        }
    }
}
