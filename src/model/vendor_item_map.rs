use super::*;

pub struct VendorBillItem;

impl VendorBillItem {
    pub async fn create_item_map(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("vendor_item_mappings")
            .find(
                doc! { "postgres_vendor": { "$exists": true }, "postgres": { "$exists": true } },
                None,
            )
            .await
            .unwrap();
        while let Some(Ok(d)) = cur.next().await {
            postgres
                .execute(
                    "INSERT INTO vendor_item_map (vendor_id, inventory_id, vendor_inventory) VALUES ($1, $2, $3) 
                    on conflict(vendor_id, vendor_inventory) do nothing",
                    &[
                        &d._get_i32("postgres_vendor").unwrap(),
                        &d._get_i32("postgres").unwrap(),
                        &format!("{}##{}", &d.get_str("vInventory").unwrap_or_default(), &d.get_str("vUnit").unwrap_or_default())
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
                    "INSERT INTO vendor_bill_map (vendor_id, start_row, name, unit, qty, mrp, rate, free, batch_no, expiry, expiry_format, discount, primary_keys)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, array['name','unit'])",
                    &[
                        &d._get_i32("postgres_vendor").unwrap(),
                        &map._get_i32("itemStartingRow").unwrap(),
                        &item_map.get_string("name").unwrap_or_default(),
                        &item_map.get_string("unit").unwrap_or_default(),
                        &item_map.get_string("qty").unwrap_or_default(),
                        &item_map.get_string("mrp").unwrap_or_default(),
                        &item_map.get_string("rate").unwrap_or_default(),
                        &item_map.get_string("free"),
                        &item_map.get_string("batchNo"),
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
