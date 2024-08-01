use super::*;

pub struct VendorBillItem;

impl VendorBillItem {
    pub async fn create_item_map(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("vendor_item_mappings")
            .aggregate(
                vec![ doc!{
                    "$match": { "postgres_vendor": { "$exists": true }, "postgres": { "$exists": true } }
                },
                doc!{
                    "$group": {
                        "_id": { "vendor_id": "$postgres_vendor", "inventory_id": "$postgres" },
                        "vendor_inventory": { "$last": "$vInventory" }
                    }
                }],
                None,
            )
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            let group_id = d._get_document("_id").unwrap();
            postgres
                .execute(
                    "INSERT INTO vendor_item_map (vendor_id, inventory_id, vendor_inventory) VALUES ($1, $2, $3)",
                    &[
                        &group_id._get_i32("vendor_id").unwrap(),
                        &group_id._get_i32("inventory_id").unwrap(),
                        &d.get_str("vendor_inventory").unwrap_or_default()
                    ],
                )
                .await
                .unwrap();
        }
    }
    pub async fn create_bill_map(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("vendor_bill_mappings")
            .find(doc! {"postgres_vendor": {"$exists": true}}, None)
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            let map = &d.get_str("mapping").unwrap_or_default();
            let map: Document = serde_json::from_str(map).unwrap();
            let item_map = map._get_document("itemMap").unwrap();
            let exp = item_map
                ._get_document("expiry")
                .and_then(|x| x.get_string("field"));
            let exp_format = item_map
                ._get_document("expiry")
                .and_then(|x| x.get_string("format"));
            postgres
                .execute(
                    "INSERT INTO vendor_bill_map (vendor_id, start_row, name, unit, qty, mrp, rate, free, batch_no, expiry, expiry_format, discount)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
                    &[
                        &d._get_i32("postgres_vendor").unwrap(),
                        &map._get_i32("itemStartingRow").unwrap(),
                        &item_map.get_string("name").unwrap_or_default(),
                        &item_map.get_string("unit").unwrap_or_default(),
                        &item_map.get_string("qty").unwrap_or_default(),
                        &item_map.get_string("mrp").unwrap_or_default(),
                        &item_map.get_string("rate").unwrap_or_default(),
                        &item_map.get_string("free"),
                        &item_map.get_string("batch_no"),
                        &exp,
                        &exp_format,
                        &item_map.get_string("discount").unwrap_or_default(),
                    ],
                )
                .await
                .unwrap();
        }
    }
}
