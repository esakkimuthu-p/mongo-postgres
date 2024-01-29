use super::*;

pub struct Inventory;

impl Inventory {
    pub async fn create(mongodb: &Database, postgres: &PostgresClient) {
        let mut cur = mongodb
            .collection::<Document>("inventories")
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
        let mut inv_updates = Vec::new();
        while let Some(Ok(d)) = cur.next().await {
            let object_id = d.get_object_id("_id").unwrap();
            let units = d.get_array_document("units").unwrap();
            let mut sale_unit = 1;
            let mut unit = 1;
            let mut purchase_unit = 1;
            let mut cess = None;
            if let Some(c) = d._get_document("cess") {
                if !c.is_empty() {
                    cess = Some(
                        serde_json::json!({ "on_value":c._get_f64("onValue"), "on_qty":  c._get_f64("onQty")}),
                    );
                }
            }
            let barcodes = d
                .get_array("barcodes")
                .map(|x| {
                    x.iter()
                        .map(|x| x.as_str().unwrap().to_string().clone())
                        .collect::<Vec<String>>()
                })
                .ok();
            let salts = d
                .get_array("postgresSalts")
                .map(|x| x.iter().map(|x| x.as_i32().unwrap()).collect::<Vec<i32>>())
                .ok();
            for u in units {
                if u._get_f64("converision").unwrap() == 1.0 {
                    unit = u.get_i32("postgresUnit").unwrap();
                }
                if u.get_bool("preferredForPurchase").unwrap_or_default() {
                    purchase_unit = u.get_i32("postgresUnit").unwrap();
                }
                if u.get_bool("preferredForSale").unwrap_or_default() {
                    sale_unit = u.get_i32("postgresUnit").unwrap();
                }
            }
            id += 1;
            postgres
                .execute(
                    "INSERT INTO inventories 
                    (id,name, division, allow_negative_stock, gst_tax, unit, sale_unit, purchase_unit,cess,
                        purchase_config, barcodes,hsn_code, description, manufacturer, manufacturer_name, 
                        vendor, vendor_name, salts, schedule_h, schedule_h1, narcotics, enable_expiry
                    ) 
                    OVERRIDING SYSTEM VALUE VALUES 
                    ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)",
                    &[
                        &id,
                        &d.get_str("name").unwrap(),
                        &d.get_i32("postgresDiv").unwrap(),
                        &d.get_bool("allowNegativeStock").unwrap_or_default(),
                        &d.get_str("tax").unwrap(),
                        &unit.clone(),
                        &sale_unit.clone(),
                        &purchase_unit.clone(),
                        &cess,
                        &serde_json::json!({"mrp_editable": true, "tax_editable": true, "free_editable": true, "disc_1_editable": true, "disc_2_editable": true, "p_rate_editable": true, "s_rate_editable": true}),
                        &barcodes,
                        &d.get_str("hsnCode").ok(),
                        &d.get_str("description").ok(),
                        &d.get_i32("postgresMan").ok(),
                        &d.get_str("manufacturerName").ok(),
                        &d.get_i32("postgresVen").ok(),
                        &d.get_str("vendorName").ok(),
                        &salts,
                        &d.get_bool("scheduleH").unwrap_or_default(),
                        &d.get_bool("scheduleH1").unwrap_or_default(),
                        &d.get_bool("narcotics").unwrap_or_default(),
                        &d.get_bool("enableExpiry").unwrap_or_default(),
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
                "u": { "$set": { "postgresManuf": id} },
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
